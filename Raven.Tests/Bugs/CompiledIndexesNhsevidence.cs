using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.Composition.Hosting;
using System.Globalization;
using System.Linq;
using System.Threading;
using Raven.Database.Indexing;
using Raven.Imports.Newtonsoft.Json;
using NLog;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Database.Linq;
using Raven.Database.Util;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	[CLSCompliant(false)]
	public class CompiledIndexesNhsevidence : RavenTest
	{
		[Fact]
		public void CanGetCorrectResults()
		{
			for (int x = 0; x < 50; x++)
			{
				Environment.SetEnvironmentVariable("Test", x.ToString(CultureInfo.InvariantCulture));
				using (var store = CreateStore())
				{
					for (int i = 0; i < 12; i++)
					{
						AddRecord(store, 5);
					}
					ReadRecords(store, 60);
				}
			}
		}

		private static void ReadRecords(IDocumentStore store, int shouldBe)
		{
			using(var session = store.OpenSession())
			{
				for (int i = 0; i < 6; i++)
				{
					int count = session.Query<object>("view" + (i + 1))
						.Customize(x => x.WaitForNonStaleResults())
						.ToList().Count;

					Assert.Equal(shouldBe, count);
				}
			}
		}

		private static void AddRecord(IDocumentStore store,int records)
		{
			var ids = new[]
			{
				"32cbecf8-6060-4099-90e0-7a2049068568",
				"121bddd7-11bf-47b6-b31f-83ef7f3d3d7b",
				"0aa639f0-fb83-45ce-89af-25812f3c2d57",
				"4a318292-f231-4b41-babe-fd769674af3d",
				"cf775c2a-e223-45fd-9dde-6c6e65b74f43",
				"a3bc9e46-f4af-4c12-bf39-023f2b6fedd0",
				"87602acb-c5ca-4e8e-9c1a-c61b3a664d5b",
				"de5916dc-48d2-4eb0-97f9-07a13d4aa750",
				"30f789f5-063d-4bcc-aadb-5e4d73313841",
				"60a581e0-c637-43f9-b0bd-4ebf32b676f9",
				"260ea926-4ce5-49f5-a8e8-3d513ad52c0c",
				"480f9271-9dba-4181-a1e7-2e3e573c6f86"
			};

			using(var session = store.OpenSession())
			{
				for (int i = 0; i < records; i++)
				{
					var item = new TestClass
					{
						Id = ids[i],
						Items = new List<Item>()
					};
					for (int j = 0; j < 5; j++)
					{
						item.Items.Add(new Item()
						{
							Id = j + 1,
							Email = string.Format("rob{0}@text.com", i + 1),
							Name = string.Format("rob{0}", i + 1)
						});
					}
					session.Store(item);
				}
				session.SaveChanges();
			}
		}

		private static EmbeddableDocumentStore CreateStore()
		{
			var store = new EmbeddableDocumentStore
			{
				Conventions = Conventions.Document,
				Configuration =
				{
					RunInMemory = true,
					MaxNumberOfParallelIndexTasks = 1
				},

			};
			store.Configuration.Catalog.Catalogs.Add(new TypeCatalog(
				typeof(View1),
				typeof(View2),
				typeof(View3),
				typeof(View4),
				typeof(View5),
				typeof(View6)
				));
			store.Initialize();
			return store;
		}

		public class Item
		{
			public int Id { get; set; }
			public string Name { get; set; }
			public string Email { get; set; }
		}

		[JsonObject(IsReference = true)]
		public class TestClass
		{
			public string Id { get; set; }
			public List<Item> Items { get; set; }
		}

		[CLSCompliant(false)]
		public class TestClassView : AbstractViewGenerator
		{
			public TestClassView()
			{

				ForEntityNames.Add("TestClasses");
				MapDefinitions.Add(MapToPaths);
				ReduceDefinition = Reduce;
				GroupByExtraction = doc => doc.UserId;

				AddField("UserId");
				AddField("Name");
				AddField("Email");

				Indexes.Add("UserId", FieldIndexing.NotAnalyzed);
			}


			private IEnumerable<dynamic> Reduce(IEnumerable<dynamic> source)
			{
				return source;
			}

			private IEnumerable<dynamic> MapToPaths(IEnumerable<dynamic> source)
			{
				foreach (var o in source)
				{
					if(o["@metadata"]["Raven-Entity-Name"] != "TestClasses")
						continue;
					var testClass = FromRaven(o);
					
					foreach (var item in testClass.Items)
					{
						yield return new
						{
							__document_id = o.Id,
							UserId = item.Id,
							item.Name,
							item.Email
						};
					}
				}
				yield break;
			}


			TestClass FromRaven(dynamic o)
			{
				var jobject = (RavenJObject)o.Inner;
				var item = ((TestClass)jobject.Deserialize(typeof(TestClass), Conventions.Document));

				if (item == null)
					throw new ApplicationException("Deserialisation failed");

				return item;
			}
		}



		public static class Conventions
		{
			public static readonly DocumentConvention Document = new DocumentConvention
			{
				FindTypeTagName = t => t.GetType() == typeof(TestClass) ? "testclass" : null,
				MaxNumberOfRequestsPerSession = 3000,
				DocumentKeyGenerator = (cmd, doc) =>
				{
					if (doc is TestClass)
						return ((TestClass)doc).Id;

					return null;
				}
			};
		}

		[DisplayName("view1")]
		public class View1 : TestClassView
		{
		}

		[DisplayName("view2")]
		public class View2 : TestClassView
		{
		
		}

		[DisplayName("view3")]
		public class View3 : TestClassView
		{
		}

		[DisplayName("view4")]
		public class View4 : TestClassView
		{
		}

		[DisplayName("view5")]
		public class View5 : TestClassView
		{
		}

		[DisplayName("view6")]
		public class View6 : TestClassView
		{
		}
	}
}