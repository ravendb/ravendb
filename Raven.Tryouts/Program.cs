using System;
using System.Net;
using Raven.Client.Document;

internal class Program
{
	private static void Main(string[] args)
	{
		using (var documntStore = new DocumentStore { Url = "http://localhost:8080", DefaultDatabase = "test" }.Initialize())
		using (var bulk = documntStore.BulkInsert())
		{
			bulk.Report += Console.WriteLine;

			for (var i = 0; i < 8000; i++)
			{
				bulk.Store(new MyItem
				{
					Hash = Guid.NewGuid().ToString("N")
				});
			}
		}
	}
}

public class MyItem
{
	public string Id { get; set; }
	public string Hash { get; set; }
}