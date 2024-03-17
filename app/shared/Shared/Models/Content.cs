namespace Shared.Models;

public class ContentData
{
	public string id{get;set;}
	
	public string content{get;set;}
	
	public string category {get; set;}
	
	public string sourcepage {get; set;}
	
	public string sourcefile {get; set;}
	
	public float[] embedding { get; set; }
	
	public float[] imageembedding { get; set; }
}