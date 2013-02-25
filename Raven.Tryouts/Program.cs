using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using System.Linq;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main(string[] args)
		{
			for (int i = 0; i < 2; i++)
			{
				using (var store = new DocumentStore
				{
					Url = "http://localhost:8080",
					DefaultDatabase = "test",
				}.Initialize())
				{
					var list = new List<string>();
					var sp = Stopwatch.StartNew();
					int start = 0;
					while (true)
					{
						var result = store.DatabaseCommands.Query("PersonList", new IndexQuery
						{
							FieldsToFetch = new[] { Constants.DocumentIdFieldName },
							PageSize = 1024,
							Start = start
						}, null);
						if (result.Results.Count == 0)
							break;
						start += result.Results.Count;
						list.AddRange(result.Results.Select(x => x.Value<string>(Constants.DocumentIdFieldName)));
					}
					sp.Stop();

					Console.WriteLine("Read all ids {0:#,#} in {1:#,#} ms", list.Count, sp.ElapsedMilliseconds);

					var rand = new Random();
					list.Sort((s, s1) => rand.Next(-1, 1));
					sp.Restart();

					foreach (var id in list)
					{
						store.DatabaseCommands.Get(id);
					}

					sp.Stop();
					Console.WriteLine("Read all docs {0:#,#} in {1:#,#} ms", list.Count, sp.ElapsedMilliseconds);
				}
			}
		} 
	}
}