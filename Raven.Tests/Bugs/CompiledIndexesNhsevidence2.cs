using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.Composition.Hosting;
using System.Globalization;
using System.Linq;
using System.Threading;
using Raven.Database.Indexing;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Database.Linq;
using Raven.Json.Linq;
using Raven.Tests;
using Xunit;

namespace Raven.Tryouts
{
	[CLSCompliant(false)]
	public class CompiledIndexesNhsevidence2 : RavenTest
	{
		[Fact(Skip = "Race condition in munin")]
		public void CanGetCorrectResults()
		{
			for (int x = 0; x < 100; x++)
			{
				Environment.SetEnvironmentVariable("Test", x.ToString(CultureInfo.InvariantCulture));
				using (var store = CreateStore())
				{
                    var test1 = IndexingUtil.MapBucket("8787bc88-d29e-4e1b-aa6a-8aa407537188");
                    var test2 = IndexingUtil.MapBucket("8787bc88-d29e-4e1b-aa6a-8aa407537187");

                    //var ids = new[]
                    //    {
                    //        "8787bc88-d29e-4e1b-aa6a-8aa407537188",
                    //        "c70b1644-0c1f-4196-aed9-e3b9a47b329c", // maps to bucket 517 (map) then to bucket 0 (level 1) //this FAILS!!!!
                    //        //"c70b1644-0c1f-4196-aed9-e3b9a47b329d", // maps to bucket 119848 (map) then to bucket 195 (level 1) //this passes!!!!!
                    //        "55d306e0-5999-4994-887b-d0c409b358b8", 
                    //        //"37a304ed-f8a9-4e89-8bb5-24979259bae5",
                    //        //"692fd6e1-ea0a-419d-8fb8-559e03e4fbfa",
                    //        //"e5a5ccfa-3ba3-47ac-a050-55f74b6bc782",
                    //        //"d2fc6934-696e-45e5-9aa2-505b0ea1f10c",
                    //        //"410f4f7b-48eb-4efa-b3e9-c314095ef45e",
                    //        //"2b3797ac-3529-4d12-99a1-f0f6da252b10",
                    //        //"2a3ff02e-8ad9-4b7c-ae68-2052c7128a9a",
                    //        //"19513de8-1692-4384-8081-5e736c9e2d30",
                    //        //"b8fccdff-4261-440e-a952-54b22a60dab6", 
                    //    };

                    var ids = new[] 
                        {
                            "cb113766-79cc-4a34-89c2-2ff66b13c9de",
                            "1aebbf97-874b-484a-9539-30cd063f0099",
                            "de4252df-4d08-4b82-842f-cf4a37a3e27d",
                            "3870f4aa-58b6-40e2-8085-c1d0d782efec",
                            "d8cf60cf-4004-4df1-b071-cd8fbcbd387e",
                            "86b2b9b6-1c2a-47d8-88f3-8575567e321d",
                            "c37fd4ec-264e-4498-9f4e-43daecdb4538",
                            "dae40ffc-4eb6-4648-82c3-8dfc6c51c772",
                            "30394662-24e0-4f12-afc4-d8f276180e30",
	                        "f4965176-41cd-404d-9320-0d668f07c5be",
	                        "19b34bfb-8c9c-4b16-a1b0-970802c6e43a",
	                        "ade6b1a4-9fff-413f-80d0-5bb228729f9a"
                        };
                    
					for (int i = 0; i < ids.Length; i++)
                    //for (int i = 0; i < 12; i++)
					{
                        var key = ids[i];
						AddRecord(store, 1, key);
                        //AddRecord(store, 1, Guid.NewGuid().ToString());
                        if (i % 3 == 0)
                            Thread.Sleep(40);
                        else
                            Thread.Sleep(5);
					}
					ReadRecords(store, ids.Length * 5);
                    //ReadRecords(store, 60);
                    //Console.ReadLine();
                    Console.Clear();
				}
			}
		}

		private static void ReadRecords(IDocumentStore store, int shouldBe)
		{
			using(var session = store.OpenSession())
			{
				//for (int i = 0; i < 6; i++)
                for (int i = 0; i < 2; i++)
				{
                    try
                    {
                        int count = session.Query<object>("view" + (i + 1))
                            .Customize(x => x.WaitForNonStaleResultsAsOfNow(TimeSpan.FromSeconds(4)))
                            .ToList().Count;

                        //Assert.Equal(shouldBe, count);
                        if (shouldBe != count)
                        {
                            Console.WriteLine("\nERROR - Expected {0}, got {1}\n", shouldBe, count);
                            Console.ReadLine();
                        }
                    }
                    catch (TimeoutException tEx)
                    {
                        Console.WriteLine("\nERROR - view{0} {1}\n", (i + 1), tEx.Message);
                        Console.ReadLine();
                    }
				}
			}
		}

		private static void AddRecord(IDocumentStore store, int records, string key)
		{			
			using (var session = store.OpenSession())
			{
				for (int i = 0; i < records; i++)
				{
					var item = new TestClass
					{
						Id = key,
						Items = new List<Item>()
					};
					for (int j = 0; j < 5; j++)
					{
						item.Items.Add(new Item
						{
							Id = j + 1,
							Email = string.Format("rob{0}@text.com", i + 1),
							Name = string.Format("rob{0}", i + 1)
						});
					}
					session.Store(item);
                    session.SaveChanges();
                    Console.WriteLine("@@@ SaveChanges() - Doc Id {0}, bucket {1}", item.Id, IndexingUtil.MapBucket(item.Id));
				}
				//session.SaveChanges();
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
                typeof(View2)
                //typeof(View3),
                //typeof(View4),
                //typeof(View5),
                //typeof(View6)
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