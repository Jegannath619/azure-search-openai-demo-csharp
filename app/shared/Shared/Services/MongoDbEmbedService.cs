// Copyright (c) Microsoft. All rights reserved.

using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using Azure;
using Azure.AI.FormRecognizer.DocumentAnalysis;
using Azure.AI.OpenAI;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Extensions.Logging;
using Shared.Models;
using MongoDB;
using MongoDB.Driver;

public sealed partial class MongoDbEmbedService(
	OpenAIClient openAIClient,
	string embeddingModelName,
	MongoClient mongoClient,
	DocumentAnalysisClient documentAnalysisClient,
	BlobContainerClient corpusContainerClient,
	IComputerVisionService? computerVisionService = null,
	bool includeImageEmbeddingsField = false,
	ILogger<MongoDbEmbedService>? logger = null) : IEmbedService
{
	[GeneratedRegex("[^0-9a-zA-Z_-]")]
	private static partial Regex MatchInSetRegex();

	public async Task<bool> EmbedPDFBlobAsync(Stream pdfBlobStream, string blobName)
	{
		try
		{
			Console.WriteLine($"Embedding blob '{blobName}'");
			var pageMap = await GetDocumentTextAsync(pdfBlobStream, blobName);

			var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(blobName);

			// Create corpus from page map and upload to blob
			// Corpus name format: fileName-{page}.txt
			foreach (var page in pageMap)
			{
				var corpusName = $"{fileNameWithoutExtension}-{page.Index}.txt";
				await UploadCorpusAsync(corpusName, page.Text);
			}

			var sections = CreateSections(pageMap, blobName);

			var infoLoggingEnabled = logger?.IsEnabled(LogLevel.Information);

			await InsertSectionsAsync(sections);

			return true;
		}
		catch (Exception exception)
		{
			logger?.LogError(
				exception, "Failed to embed blob '{BlobName}'", blobName);

			throw;
		}
	}

	public async Task<bool> EmbedImageBlobAsync(
		Stream imageStream,
		string imageUrl,
		string imageName,
		CancellationToken ct = default)
	{
		if (includeImageEmbeddingsField == false || computerVisionService is null)
		{
			throw new InvalidOperationException(
				"Computer Vision service is required to include image embeddings field, please enable GPT_4V support");
		}

		var embeddings = await computerVisionService.VectorizeImageAsync(imageUrl, ct);

		// id can only contain letters, digits, underscore (_), dash (-), or equal sign (=).
		var imageId = MatchInSetRegex().Replace(imageUrl, "_").TrimStart('_');
		// step 3
		// index image embeddings
		var contentData = new ContentData
			{
				id = imageId,
				content = imageName,
				category = "image",
				imageembedding = embeddings.vector,
				sourcefile = imageUrl,
			};

		var database = mongoClient.GetDatabase("chatbots");
		var collection = database.GetCollection<ContentData>("chatbot1");
		await collection.InsertOneAsync(contentData);
		return true;
	}

	public async Task<IReadOnlyList<PageDetail>> GetDocumentTextAsync(Stream blobStream, string blobName)
	{
		logger?.LogInformation(
			"Extracting text from '{Blob}' using Azure Form Recognizer", blobName);

		using var ms = new MemoryStream();
		blobStream.CopyTo(ms);
		ms.Position = 0;
		AnalyzeDocumentOperation operation = documentAnalysisClient.AnalyzeDocument(
			WaitUntil.Started, "prebuilt-layout", ms);

		var offset = 0;
		List<PageDetail> pageMap = [];

		var results = await operation.WaitForCompletionAsync();
		var pages = results.Value.Pages;
		for (var i = 0; i < pages.Count; i++)
		{
			IReadOnlyList<DocumentTable> tablesOnPage =
				results.Value.Tables.Where(t => t.BoundingRegions[0].PageNumber == i + 1).ToList();

			// Mark all positions of the table spans in the page
			int pageIndex = pages[i].Spans[0].Index;
			int pageLength = pages[i].Spans[0].Length;
			int[] tableChars = Enumerable.Repeat(-1, pageLength).ToArray();
			for (var tableId = 0; tableId < tablesOnPage.Count; tableId++)
			{
				foreach (DocumentSpan span in tablesOnPage[tableId].Spans)
				{
					// Replace all table spans with "tableId" in tableChars array
					for (var j = 0; j < span.Length; j++)
					{
						int index = span.Index - pageIndex + j;
						if (index >= 0 && index < pageLength)
						{
							tableChars[index] = tableId;
						}
					}
				}
			}

			// Build page text by replacing characters in table spans with table HTML
			StringBuilder pageText = new();
			HashSet<int> addedTables = [];
			for (int j = 0; j < tableChars.Length; j++)
			{
				if (tableChars[j] == -1)
				{
					pageText.Append(results.Value.Content[pageIndex + j]);
				}
				else if (!addedTables.Contains(tableChars[j]))
				{
					pageText.Append(TableToHtml(tablesOnPage[tableChars[j]]));
					addedTables.Add(tableChars[j]);
				}
			}

			pageText.Append(' ');
			pageMap.Add(new PageDetail(i, offset, pageText.ToString()));
			offset += pageText.Length;
		}
		return pageMap.AsReadOnly();
	}

	private static string TableToHtml(DocumentTable table)
	{
		var tableHtml = new StringBuilder("<table>");
		var rows = new List<DocumentTableCell>[table.RowCount];
		for (int i = 0; i < table.RowCount; i++)
		{
			rows[i] =
			[
				.. table.Cells.Where(c => c.RowIndex == i)
								.OrderBy(c => c.ColumnIndex)
,
			];
		}

		foreach (var rowCells in rows)
		{
			tableHtml.Append("<tr>");
			foreach (DocumentTableCell cell in rowCells)
			{
				var tag = (cell.Kind == "columnHeader" || cell.Kind == "rowHeader") ? "th" : "td";
				var cellSpans = string.Empty;
				if (cell.ColumnSpan > 1)
				{
					cellSpans += $" colSpan='{cell.ColumnSpan}'";
				}

				if (cell.RowSpan > 1)
				{
					cellSpans += $" rowSpan='{cell.RowSpan}'";
				}

				tableHtml.AppendFormat(
					"<{0}{1}>{2}</{0}>", tag, cellSpans, WebUtility.HtmlEncode(cell.Content));
			}

			tableHtml.Append("</tr>");
		}

		tableHtml.Append("</table>");

		return tableHtml.ToString();
	}

	private async Task UploadCorpusAsync(string corpusBlobName, string text)
	{
		var blobClient = corpusContainerClient.GetBlobClient(corpusBlobName);
		if (await blobClient.ExistsAsync())
		{
			return;
		}

		logger?.LogInformation("Uploading corpus '{CorpusBlobName}'", corpusBlobName);

		await using var stream = new MemoryStream(Encoding.UTF8.GetBytes(text));
		await blobClient.UploadAsync(stream, new BlobHttpHeaders
		{
			ContentType = "text/plain"
		});
	}

	public IEnumerable<Section> CreateSections(
		IReadOnlyList<PageDetail> pageMap, string blobName)
	{
		const int MaxSectionLength = 5_00;
		const int SentenceSearchLimit = 100;
		const int SectionOverlap = 100;

		var sentenceEndings = new[] { '.', '!', '?' };
		var wordBreaks = new[] { ',', ';', ':', ' ', '(', ')', '[', ']', '{', '}', '\t', '\n' };
		var allText = string.Concat(pageMap.Select(p => p.Text));
		var length = allText.Length;
		var start = 0;
		var end = length;

		logger?.LogInformation("Splitting '{BlobName}' into sections", blobName);

		while (start + SectionOverlap < length)
		{
			var lastWord = -1;
			end = start + MaxSectionLength;

			if (end > length)
			{
				end = length;
			}
			else
			{
				// Try to find the end of the sentence
				while (end < length && (end - start - MaxSectionLength) < SentenceSearchLimit && !sentenceEndings.Contains(allText[end]))
				{
					if (wordBreaks.Contains(allText[end]))
					{
						lastWord = end;
					}
					end++;
				}

				if (end < length && !sentenceEndings.Contains(allText[end]) && lastWord > 0)
				{
					end = lastWord; // Fall back to at least keeping a whole word
				}
			}

			if (end < length)
			{
				end++;
			}

			// Try to find the start of the sentence or at least a whole word boundary
			lastWord = -1;
			while (start > 0 && start > end - MaxSectionLength -
				(2 * SentenceSearchLimit) && !sentenceEndings.Contains(allText[start]))
			{
				if (wordBreaks.Contains(allText[start]))
				{
					lastWord = start;
				}
				start--;
			}

			if (!sentenceEndings.Contains(allText[start]) && lastWord > 0)
			{
				start = lastWord;
			}
			if (start > 0)
			{
				start++;
			}

			var sectionText = allText[start..end];

			yield return new Section(
				Id: MatchInSetRegex().Replace($"{blobName}-{start}", "_").TrimStart('_'),
				Content: sectionText,
				SourcePage: BlobNameFromFilePage(blobName, FindPage(pageMap, start)),
				SourceFile: blobName);

			var lastTableStart = sectionText.LastIndexOf("<table", StringComparison.Ordinal);
			if (lastTableStart > 2 * SentenceSearchLimit && lastTableStart > sectionText.LastIndexOf("</table", StringComparison.Ordinal))
			{
				// If the section ends with an unclosed table, we need to start the next section with the table.
				// If table starts inside SentenceSearchLimit, we ignore it, as that will cause an infinite loop for tables longer than MaxSectionLength
				// If last table starts inside SectionOverlap, keep overlapping
				if (logger?.IsEnabled(LogLevel.Warning) is true)
				{
					logger?.LogWarning("""
						Section ends with unclosed table, starting next section with the
						table at page {Offset} offset {Start} table start {LastTableStart}
						""",
						FindPage(pageMap, start),
						start,
						lastTableStart);
				}

				start = Math.Min(end - SectionOverlap, start + lastTableStart);
			}
			else
			{
				start = end - SectionOverlap;
			}
		}

		if (start + SectionOverlap < end)
		{
			yield return new Section(
				Id: MatchInSetRegex().Replace($"{blobName}-{start}", "_").TrimStart('_'),
				Content: allText[start..end],
				SourcePage: BlobNameFromFilePage(blobName, FindPage(pageMap, start)),
				SourceFile: blobName);
		}
	}

	private static int FindPage(IReadOnlyList<PageDetail> pageMap, int offset)
	{
		var length = pageMap.Count;
		for (var i = 0; i < length - 1; i++)
		{
			if (offset >= pageMap[i].Offset && offset < pageMap[i + 1].Offset)
			{
				return i;
			}
		}

		return length - 1;
	}

	private static string BlobNameFromFilePage(string blobName, int page = 0) => blobName;

	private async Task InsertSectionsAsync(IEnumerable<Section> sections)
	{
		var database = mongoClient.GetDatabase("chatbots");
		var collection = database.GetCollection<ContentData>("chatbot1");
		var contentDatas = new List<ContentData>();
		foreach (var section in sections)
		{
			
			var embeddings = await openAIClient.GetEmbeddingsAsync(new Azure.AI.OpenAI.EmbeddingsOptions(embeddingModelName, [section.Content.Replace('\r', ' ')]));
			var embedding = embeddings.Value.Data.FirstOrDefault()?.Embedding.ToArray() ?? [];
			var contentData = new ContentData{
				id= section.Id,
				content = section.Content,
				category = section.Category,
				sourcepage= section.SourcePage,
				sourcefile= section.SourceFile,
				embedding = embedding
			};
			contentDatas.Add(contentData);
		}
		await collection.InsertManyAsync(contentDatas);

	}
}
