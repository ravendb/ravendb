using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using System.Linq;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main()
		{
			if (Directory.Exists("Data"))
				Directory.Delete("Data", true);

			using (var documentStore = new DocumentStore())
			{
				documentStore.Configuration.DataDirectory = "Data";
				documentStore.Initialize();

				documentStore.DatabaseCommands.PutIndex("Foo1", new IndexDefinition<Foo>
				{
					Map = docs => from doc in docs select new { doc.PropertyA }
				});
				documentStore.DatabaseCommands.PutIndex("Foo2", new IndexDefinition<Foo>
				{
					Map = docs => from doc in docs select new { doc.PropertyA }
				});
				documentStore.DatabaseCommands.PutIndex("Foo3", new IndexDefinition<Foo>
				{
					Map = docs => from doc in docs select new { doc.PropertyA }
				});

				Thread.Sleep(5000); // just in case there are any background tasks still running

				for (var i = 1; i <= 128; i++)
				{
					InsertNewDocumentsAndWaitForStaleIndexes(documentStore, i);
				}
				for (var i = 2; i <= 30; i++)
				{
					InsertNewDocumentsAndWaitForStaleIndexes(documentStore, i * 128);
				}
			}
		}

		private static void InsertNewDocumentsAndWaitForStaleIndexes(DocumentStore documentStore, int numberOfDocs)
		{
			var stopwatch = Stopwatch.StartNew();
			var docsToWrite = numberOfDocs;
			while (docsToWrite > 0)
			{
				using (var session = documentStore.OpenSession())
				{
					for (var i = 0; (numberOfDocs > 0) && (i < 128); i++, docsToWrite--)
					{
						session.Store(new Foo { PropertyA = "abc def geh" });
					}
					session.SaveChanges();
				}
			}
			var insertTime = stopwatch.ElapsedMilliseconds;
			stopwatch.Restart();
			using (var session = documentStore.OpenSession())
			{
				foreach (var index in documentStore.DocumentDatabase.GetIndexNames(0, int.MaxValue))
				{
					var indexName = ((JValue)index).Value as string;
					session.LuceneQuery<object>(indexName).WaitForNonStaleResults(TimeSpan.MaxValue)
						.Take(1)
						.FirstOrDefault();
				}
			}
			var indexingTime = stopwatch.ElapsedMilliseconds;
			Console.WriteLine("{0}, {1}, {2}", numberOfDocs, insertTime, indexingTime);
		}
	}

	public class Foo
	{
		public string Id { get; set; }
		public string PropertyA { get; set; }
	}
}