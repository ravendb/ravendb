//-----------------------------------------------------------------------
// <copyright file="Intersection.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Linq;
using Xunit;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;

namespace Raven.Tests.Queries
{   
	public class IntersectionQuery : RavenTest
	{
		[Fact]
		public void CanPerformIntersectionQuery_Remotely()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				ExecuteTest(store);
			}
		}

		[Fact]
		public void CanPeformIntersectionQuery_Embedded()
		{
			using (var store = NewDocumentStore())
			{
				ExecuteTest(store);
			}
		}

		[Fact]
		public void CanPerformIntersectionQuery_Linq()
		{
			using (var store = NewDocumentStore())
			{
				CreateIndexAndSampleData(store);

				using(var session = store.OpenSession())
				{
					var shirts = session.Query<TShirt>("TShirtNested")
						.OrderBy(x=>x.BarcodeNumber)
						.Where(x => x.Name == "Wolf")
						.Intersect()
						.Where(x => x.Types.Any(t => t.Colour == "Blue" && t.Size == "Small"))
						.Intersect()
						.Where(x => x.Types.Any(t => t.Colour == "Gray" && t.Size == "Large"))
						.ToList();

					Assert.Equal(6, shirts.Count);
					Assert.True(shirts.All(x => x.Name == "Wolf"));
					Assert.Equal(new[] { -999, 10001, 10002, 10003, 10004, 10006 }, shirts.Select(x=>x.BarcodeNumber));
				}
			}
		}

		private void ExecuteTest(IDocumentStore store)
		{
			CreateIndexAndSampleData(store);

			using (var s = store.OpenSession())
			{
				//This should be BarCodeNumber = -999, 10001
				var resultPage1 = s.Advanced.LuceneQuery<TShirt>("TShirtNested")
					.Where("Name:Wolf INTERSECT Types_Colour:Blue AND Types_Size:Small INTERSECT Types_Colour:Gray AND Types_Size:Large")
					.OrderBy("BarcodeNumber")
					.Take(2)
					.ToList();
				Assert.Equal(2, resultPage1.Count);
				Assert.True(resultPage1.All(x => x.Name == "Wolf"));
				foreach (var result in resultPage1)
				{
					Assert.True(result.Types.Any(x => x.Colour == "Gray" && x.Size == "Large"));
					Assert.True(result.Types.Any(x => x.Colour == "Blue" && x.Size == "Small"));
				}
				Assert.Equal(new[] { -999, 10001 }, resultPage1.Select(r => r.BarcodeNumber));

				//This should be BarCodeNumber = 10001, 10002 (i.e. it spans pages 1 & 2)
				var resultPage1a = s.Advanced.LuceneQuery<TShirt>("TShirtNested")
					.Where("Name:Wolf INTERSECT Types_Colour:Blue AND Types_Size:Small INTERSECT Types_Colour:Gray AND Types_Size:Large")
					.OrderBy("BarcodeNumber")
					.Skip(1)
					.Take(2)
					.ToList();
				Assert.Equal(2, resultPage1a.Count);
				Assert.True(resultPage1a.All(x => x.Name == "Wolf"));
				foreach (var result in resultPage1a)
				{
					Assert.True(result.Types.Any(x => x.Colour == "Gray" && x.Size == "Large"));
					Assert.True(result.Types.Any(x => x.Colour == "Blue" && x.Size == "Small"));
				}
				Assert.Equal(new[] { 10001, 10002 }, resultPage1a.Select(r => r.BarcodeNumber));

				//This should be BarCodeNumber = 10002, 10003, 10004, 10006 (But NOT 10005
				var resultPage2 = s.Advanced.LuceneQuery<TShirt>("TShirtNested")
					.Where("Name:Wolf INTERSECT Types_Colour:Blue AND Types_Size:Small INTERSECT Types_Colour:Gray AND Types_Size:Large")
					.OrderBy("BarcodeNumber")
					.Skip(2)
					.Take(10) //we should only get 4 here, want to test a page size larger than what is possible!!!!!
					.ToList();
				Assert.Equal(4, resultPage2.Count);
				Assert.True(resultPage2.All(x => x.Name == "Wolf"));
				foreach (var result in resultPage2)
				{
					Assert.True(result.Types.Any(x => x.Colour == "Gray" && x.Size == "Large"));
					Assert.True(result.Types.Any(x => x.Colour == "Blue" && x.Size == "Small"));
				}
				Assert.Equal(new[] { 10002, 10003, 10004, 10006 }, resultPage2.Select(r => r.BarcodeNumber));
			}
		}

		private void CreateIndexAndSampleData(IDocumentStore store)
		{
			using (var s = store.OpenSession())
			{
				store.DatabaseCommands.PutIndex("TShirtNested",
				                                new IndexDefinition
				                                {
				                                	Map =
				                                	@"from s in docs.TShirts
															from t in s.Types
															select new { s.Name, Types_Colour = t.Colour, Types_Size = t.Size, s.BarcodeNumber }",
				                                	SortOptions =
				                                	new Dictionary<String, SortOptions> {{"BarcodeNumber", SortOptions.Int}}
				                                });

				foreach (var sample in GetSampleData())
				{
					s.Store(sample);
				}
				s.SaveChanges();
			}

			WaitForIndexing(store);
		}

		private IEnumerable<TShirt> GetSampleData()
		{
			var tShirt1 = new TShirt
			{
				Name = "Wolf",
				BarcodeNumber = 10001,
				Types = new List<TShirtType>
					{
						new TShirtType { Colour = "Blue",  Size = "Small" },
						new TShirtType { Colour = "Black", Size = "Small" },
						new TShirtType { Colour = "Black", Size = "Medium" },
						new TShirtType { Colour = "Gray",  Size = "Large" }
					}
			};
			var tShirt2 = new TShirt
			{
				Name = "Wolf",
				BarcodeNumber = 1,
				Types = new List<TShirtType>
								{
									new TShirtType { Colour = "Blue",  Size = "Small" },                                    
									new TShirtType { Colour = "Black", Size = "Large" },
									new TShirtType { Colour = "Gray",  Size = "Medium" }
								}
			};
			var tShirt3 = new TShirt
			{
				Name = "Owl",
				BarcodeNumber = 99999,
				Types = new List<TShirtType>
								{
									new TShirtType { Colour = "Blue",  Size = "Small" },
									new TShirtType { Colour = "Gray",  Size = "Medium" }
								}
			};
			var tShirt4 = new TShirt
			{
				Name = "Wolf",
				BarcodeNumber = -999,
				Types = new List<TShirtType>
					{
						new TShirtType { Colour = "Blue",  Size = "Small" },
						new TShirtType { Colour = "Black", Size = "Small" },
						new TShirtType { Colour = "Black", Size = "Medium" },
						new TShirtType { Colour = "Gray",  Size = "Large" }
					}
			};
			var tShirt5 = new TShirt
			{
				Name = "Wolf",
				BarcodeNumber = 10002,
				Types = new List<TShirtType>
								{
									new TShirtType { Colour = "Blue",  Size = "Small" },
									new TShirtType { Colour = "Gray",  Size = "Large" }
								}
			};
			var tShirt6 = new TShirt
			{
				Name = "Wolf",
				BarcodeNumber = 10003,
				Types = new List<TShirtType>
								{
									new TShirtType { Colour = "Blue",  Size = "Small" },
									new TShirtType { Colour = "Gray",  Size = "Large" }
								}
			};
			var tShirt7 = new TShirt
			{
				Name = "Wolf",
				BarcodeNumber = 10004,
				Types = new List<TShirtType>
								{
									new TShirtType { Colour = "Blue",  Size = "Small" },
									new TShirtType { Colour = "Gray",  Size = "Large" }
								}
			};

			var tShirt8 = new TShirt
			{
				Name = "Wolf",
				BarcodeNumber = 10005,
				Types = new List<TShirtType> //Doesn't MAtch SUB-QUERIES
								{
									new TShirtType { Colour = "Blue",  Size = "Small" },
									new TShirtType { Colour = "Gray",  Size = "Medium" }
								}
			};

			var tShirt9 = new TShirt
			{
				Name = "Wolf",
				BarcodeNumber = 10006,
				Types = new List<TShirtType>
								{
									new TShirtType { Colour = "Blue",  Size = "Small" },
									new TShirtType { Colour = "Gray",  Size = "Large" }
								}
			};

			yield return tShirt1;
			yield return tShirt2;
			yield return tShirt3;
			yield return tShirt4;
			yield return tShirt5;
			yield return tShirt6;
			yield return tShirt7;
			yield return tShirt8;
			yield return tShirt9;
		}

		public class TShirt
		{
			public String Id { get; set; }
			public String Name { get; set; }
			public int BarcodeNumber { get; set; }
			public List<TShirtType> Types { get; set; }
		}

		public class TShirtType
		{
			public String Colour { get; set; }
			public String Size { get; set; }

			public override string ToString()
			{
				return String.Format("{{{0}, {1}}}", Colour, Size);
			}
		}
	}
}
