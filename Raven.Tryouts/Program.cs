using System;
using System.IO;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Embedded;
using Raven.Client.Indexes;

namespace Raven.Tryouts
{
	class Program
	{
		private static void Main(string[] args)
		{
			var emptyDB = !Directory.Exists("MyData2");
			using (var documentStore = new EmbeddableDocumentStore
			{
				Configuration =
				{
					DataDirectory = "MyData2",
				},
				UseEmbeddedHttpServer = true
			})
			{
				documentStore.Initialize();
				if (emptyDB) SetupSampleData(documentStore);
				
				using (var session = documentStore.OpenSession())
				{
					var query = session.Advanced.LuceneQuery<Foo>(new FooIndex().IndexName);

					// Docs are stored in session by default!!!
					// query.NoTracking();

					QueryHeaderInformation stats;
					var enumerator = session.Advanced.Stream(query, out stats);
					var count = 0;
					while (enumerator.MoveNext())
					{
						count++;
						if (count % 100 == 0) Console.WriteLine(count);

						// If we don't consume the docs fast enough, they will eat all your memory
						// Thread.Sleep(1000);
					}
				}
			}
			Console.ReadLine();
		}

		private static void SetupSampleData(EmbeddableDocumentStore documentStore)
		{
			new FooIndex().Execute(documentStore);

			for (var j = 0; j < 1000; j++)
			{
				using (var session = documentStore.OpenSession())
				{
					for (var i = 0; i < 128; i++)
					{
						var foo = new Foo { Something = new Guid().ToString(), Payload = new string('x', 10 * 1024) };
						session.Store(foo);
					}
					session.SaveChanges();
				}
			}
		}

		public class FooIndex : AbstractIndexCreationTask<Foo>
		{
			public FooIndex()
			{
				Map = foos => from foo in foos select new { foo.Something };
			}
		}

		public class Foo
		{
			public string Id { get; set; }
			public string Something { get; set; }
			public string Payload { get; set; }
		}
	}
}