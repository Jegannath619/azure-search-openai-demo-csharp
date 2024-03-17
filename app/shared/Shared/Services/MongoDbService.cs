using Azure;
using Shared.Models;
using MongoDB;
using MongoDB.Driver;

public class MongoDbService(MongoClient mongoClient) : ISearchService
{
	public async Task<SupportingContentRecord[]> QueryDocumentsAsync(
		string? query = null,
		float[]? embedding = null,
		RequestOverrides? overrides = null,
		CancellationToken cancellationToken = default)
	{
		if (query is null && embedding is null)
		{
			throw new ArgumentException("Either query or embedding must be provided");
		}

		var documentContents = string.Empty;
		var top = overrides?.Top ?? 3;
		var exclude_category = overrides?.ExcludeCategory;
		var filter = exclude_category == null ? string.Empty : $"category ne '{exclude_category}'";
		var useSemanticRanker = overrides?.SemanticRanker ?? false;
		var useSemanticCaptions = overrides?.SemanticCaptions ?? false;

		var vectorOptions = new VectorSearchOptions<ContentData>()
		{
			IndexName = "vector_index",
			NumberOfCandidates = 150
		};
		var database = mongoClient.GetDatabase("chatbots");
		var collection = database.GetCollection<ContentData>("chatbot1");
		var contentDatas = collection.Aggregate()
			.VectorSearch(movie => movie.embedding, embedding, 150, vectorOptions)
			.Project<ContentData>(Builders<ContentData>.Projection
			.Include(m => m.id)
			.Include(m => m.content)
			.Include(m => m.category)
			.Include(m => m.sourcepage)
			.Include(m => m.sourcefile))  	  
			.ToList();
		

		if (contentDatas is null)
		{
			throw new InvalidOperationException("fail to get search result");
		}


		// Assemble sources here.
		// Example output for each SearchDocument:
		// {
		//   "@search.score": 11.65396,
		//   "id": "Northwind_Standard_Benefits_Details_pdf-60",
		//   "content": "x-ray, lab, or imaging service, you will likely be responsible for paying a copayment or coinsurance. The exact amount you will be required to pay will depend on the type of service you receive. You can use the Northwind app or website to look up the cost of a particular service before you receive it.\nIn some cases, the Northwind Standard plan may exclude certain diagnostic x-ray, lab, and imaging services. For example, the plan does not cover any services related to cosmetic treatments or procedures. Additionally, the plan does not cover any services for which no diagnosis is provided.\nIt’s important to note that the Northwind Standard plan does not cover any services related to emergency care. This includes diagnostic x-ray, lab, and imaging services that are needed to diagnose an emergency condition. If you have an emergency condition, you will need to seek care at an emergency room or urgent care facility.\nFinally, if you receive diagnostic x-ray, lab, or imaging services from an out-of-network provider, you may be required to pay the full cost of the service. To ensure that you are receiving services from an in-network provider, you can use the Northwind provider search ",
		//   "category": null,
		//   "sourcepage": "Northwind_Standard_Benefits_Details-24.pdf",
		//   "sourcefile": "Northwind_Standard_Benefits_Details.pdf"
		// }
		var sb = new List<SupportingContentRecord>();
		foreach (var doc in contentDatas)
		{
			string? contentValue;
			try
			{
				contentValue = (string)doc.content;
			}
			catch (ArgumentNullException)
			{
				contentValue = null;
			}

			if (doc.sourcepage is string sourcePage && contentValue is string content)
			{
				content = content.Replace('\r', ' ').Replace('\n', ' ');
				sb.Add(new SupportingContentRecord(sourcePage, content));
			}
		}

		return [.. sb];
	}

	/// <summary>
	/// query images.
	/// </summary>
	/// <param name="embedding">embedding for imageEmbedding</param>
	public async Task<SupportingImageRecord[]> QueryImagesAsync(
		string? query = null,
		float[]? embedding = null,
		RequestOverrides? overrides = null,
		CancellationToken cancellationToken = default)
	{
		var top = overrides?.Top ?? 3;
		var exclude_category = overrides?.ExcludeCategory;
		var filter = exclude_category == null ? string.Empty : $"category ne '{exclude_category}'";

		var vectorOptions = new VectorSearchOptions<ContentData>()
		{
			IndexName = "image_vector_index",
			NumberOfCandidates = 150
		};
		var database = mongoClient.GetDatabase("chatbots");
		var collection = database.GetCollection<ContentData>("chatbot1");
		var contentDatas = collection.Aggregate()
			.VectorSearch(con => con.imageembedding, embedding, 150, vectorOptions)
			.Project<ContentData>(Builders<ContentData>.Projection
			.Include(m => m.id)
			.Include(m => m.content)
			.Include(m => m.category)
			.Include(m => m.imageembedding)
			.Include(m => m.sourcefile))  	  
			.ToList();
		

		if (contentDatas is null)
		{
			throw new InvalidOperationException("fail to get search result");
		}

		var sb = new List<SupportingImageRecord>();

		foreach (var doc in contentDatas)
		{
			if (doc.sourcefile is string url &&
				doc.content is string name &&
				doc.category is string category &&
				category == "image")
			{
				sb.Add(new SupportingImageRecord(name, url));
			}
		}

		return [.. sb];
	}
}
