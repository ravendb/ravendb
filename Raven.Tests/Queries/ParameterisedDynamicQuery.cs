//-----------------------------------------------------------------------
// <copyright file="ParameterisedDynamicQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Client.Embedded;
using Raven.Database.Config;
using Raven.Database.Queries;
using Raven.Tests.Storage;
using Xunit;
using Raven.Database;
using Raven.Json.Linq;

namespace Raven.Tests.Queries
{
	public class ParameterisedDynamicQuery : RavenTest
	{
		private readonly EmbeddableDocumentStore store;
		private readonly DocumentDatabase db;

		public ParameterisedDynamicQuery()
		{
			store = NewDocumentStore();
			db = store.DocumentDatabase;
		}

		public override void Dispose()
		{
			store.Dispose();
			base.Dispose();
		}

		[Fact]
		public void CanPerformDynamicQueryAndGetValidResults()
		{
			var blogOne = new Blog
			{
				Title = "one",
				Category = "Ravens"
			};
			var blogTwo = new Blog
			{
				Title = "two",
				Category = "Rhinos"
			};
			var blogThree = new Blog
			{
				Title = "three",
				Category = "Rhinos"
			};

			db.Put("blogOne", null, RavenJObject.FromObject(blogOne), new RavenJObject(), null);
			db.Put("blogTwo", null, RavenJObject.FromObject(blogTwo), new RavenJObject(), null);
			db.Put("blogThree", null, RavenJObject.FromObject(blogThree), new RavenJObject(), null);

			var results = db.ExecuteDynamicQuery(null,new IndexQuery()
		   {
			   PageSize = 128,
			   Start = 0,
			   Cutoff = SystemTime.UtcNow,
			   Query = "Title.Length:3 AND Category:Rhinos"
		   });

			Assert.Equal(1, results.Results.Count);
			Assert.Equal("two", results.Results[0].Value<string>("Title"));
			Assert.Equal("Rhinos", results.Results[0].Value<string>("Category"));
		}

		[Fact]
		public void SimpleQueriesDoNotGeneratedMultipleIndexes()
		{
			var blogOne = new Blog
			{
				Title = "one",
				Category = "Ravens"
			};
			var blogTwo = new Blog
			{
				Title = "two",
				Category = "Rhinos"
			};
			var blogThree = new Blog
			{
				Title = "three",
				Category = "Rhinos"
			};

			db.Put("blogOne", null, RavenJObject.FromObject(blogOne), new RavenJObject(), null);
			db.Put("blogTwo", null, RavenJObject.FromObject(blogTwo), new RavenJObject(), null);
			db.Put("blogThree", null, RavenJObject.FromObject(blogThree), new RavenJObject(), null);

			int initialIndexCount = db.Statistics.CountOfIndexes;
			db.ExecuteDynamicQuery(null,new IndexQuery()
			{
				PageSize = 128,
				Start = 0,
				Cutoff = SystemTime.UtcNow,
				Query = "Title.Length:3 AND Category:Rhinos"
			});
			db.ExecuteDynamicQuery(null, new IndexQuery()
			{
				PageSize = 128,
				Start = 0,
				Cutoff = SystemTime.UtcNow,
				Query = "Title.Length:3 AND Category:Rhinos"
			});
			db.ExecuteDynamicQuery(null, new IndexQuery()
			{
				PageSize = 128,
				Start = 0,
				Cutoff = SystemTime.UtcNow,
				Query = "Category:Rhinos AND Title.Length:3"
			});

			Assert.True(db.Statistics.CountOfIndexes == initialIndexCount + 1);
						
		}

		[Fact]
		public void SingleInvokedQueryShouldCreateOnlyCreatedTempIndex()
		{
			int initialIndexCount = db.Statistics.CountOfIndexes;


			db.ExecuteDynamicQuery(null, new IndexQuery()
				{
					PageSize = 128,
					Start = 0,
					Cutoff = SystemTime.UtcNow,
					Query = "Title.Length:3 AND Category:Rhinos"
				});
		  

			var autoIndexName = db.IndexDefinitionStorage.IndexNames.Where(x => x.StartsWith("Auto")).SingleOrDefault();
			var tempIndexName = db.IndexDefinitionStorage.IndexNames.Where(x => x.StartsWith("Temp")).SingleOrDefault();

			Assert.False(string.IsNullOrEmpty(tempIndexName));
			Assert.True(string.IsNullOrEmpty(autoIndexName));
		}

		[Fact]
		public void LengthPropertySupportsRangedQueries()
		{
			var blogOne = new Blog
			{
				Title = "one",
				Category = "Ravens"
			};
			var blogTwo = new Blog
			{
				Title = "two",
				Category = "Rhinos"
			};
			var blogThree = new Blog
			{
				Title = "three",
				Category = "Rhinos"
			};

			db.Put("blogOne", null, RavenJObject.FromObject(blogOne), new RavenJObject(), null);
			db.Put("blogTwo", null, RavenJObject.FromObject(blogTwo), new RavenJObject(), null);
			db.Put("blogThree", null, RavenJObject.FromObject(blogThree), new RavenJObject(), null);

			QueryResult results = null;

			results = db.ExecuteDynamicQuery(null, new IndexQuery()
				{
					PageSize = 128,
					Start = 0,
					Cutoff = SystemTime.UtcNow,
					Query = "Title.Length_Range:[0x00000004 TO 0x00000009]"
				});

			Assert.Equal(1, results.TotalResults);
		}
		[Fact]
		public void OftenInvokedQueryShouldCreatePermanentIndex()
		{
			db.Configuration.TempIndexPromotionMinimumQueryCount = 2;
			db.Configuration.TempIndexPromotionThreshold = 2000;

			for (int x = 0; x < 4; x++)
			{
				db.ExecuteDynamicQuery(null, new IndexQuery()
				{
					PageSize = 128,
					Start = 0,
					Cutoff = SystemTime.UtcNow,
					Query = "Title.Length:3 AND Category:Rhinos"
				});
			}

			
			var autoIndexName = db.IndexDefinitionStorage.IndexNames.Where(x => x.StartsWith("Auto")).SingleOrDefault();
			var tempIndexName = db.IndexDefinitionStorage.IndexNames.Where(x => x.StartsWith("Temp")).SingleOrDefault();

			Assert.True(string.IsNullOrEmpty(tempIndexName));
			Assert.False(string.IsNullOrEmpty(autoIndexName));
		}

		[Fact]
		public void NestedCollectionPropertiesCanBeQueried()
		{
			var blogOne = new Blog
			{
				Title = "one",
				Category = "Ravens",
				Tags = new Tag[]{
					 new Tag(){ Name = "birds" }
				},
			};
			var blogTwo = new Blog
			{
				Title = "two",
				Category = "Rhinos",
				Tags = new Tag[]{
					 new Tag(){ Name = "mammals" }
				},
			};
			var blogThree = new Blog
			{
				Title = "three",
				Category = "Rhinos",
				Tags = new Tag[]{
					 new Tag(){ Name = "mammals" }
				},
			};

			db.Put("blogOne", null, RavenJObject.FromObject(blogOne), new RavenJObject(), null);
			db.Put("blogTwo", null, RavenJObject.FromObject(blogTwo), new RavenJObject(), null);
			db.Put("blogThree", null, RavenJObject.FromObject(blogThree), new RavenJObject(), null);

			var results = db.ExecuteDynamicQuery(null, new IndexQuery()
			{
				PageSize = 128,
				Start = 0,
				Cutoff = SystemTime.UtcNow,
				Query = "Tags,Name:[[birds]]"
			});

			Assert.Equal(1, results.Results.Count);
			Assert.Equal("one", results.Results[0].Value<string>("Title"));
			Assert.Equal("Ravens", results.Results[0].Value<string>("Category"));
		}
		
		[Fact]
		public void NestedPropertiesCanBeQueried()
		{
			var blogOne = new Blog
			{
				Title = "one",
				Category = "Ravens",
				User = new User(){ Name = "ayende" }
			};
			var blogTwo = new Blog
			{
				Title = "two",
				Category = "Rhinos",
				User = new User() { Name = "ayende" }
			};
			var blogThree = new Blog
			{
				Title = "three",
				Category = "Rhinos",
				User = new User() { Name = "rob" }
			};

			db.Put("blogOne", null, RavenJObject.FromObject(blogOne), new RavenJObject(), null);
			db.Put("blogTwo", null, RavenJObject.FromObject(blogTwo), new RavenJObject(), null);
			db.Put("blogThree", null, RavenJObject.FromObject(blogThree), new RavenJObject(), null);

			var results = db.ExecuteDynamicQuery(null, new IndexQuery()
			{
				PageSize = 128,
				Start = 0,
				Cutoff = SystemTime.UtcNow,
				Query = "User.Name:rob"
			});

			Assert.Equal(1, results.Results.Count);
			Assert.Equal("three", results.Results[0].Value<string>("Title"));
			Assert.Equal("Rhinos", results.Results[0].Value<string>("Category"));
		}

		[Fact]
		public void NestedCollectionPropertiesCanBeQueriedWithProjection()
		{
			var blogOne = new Blog
			{
				Title = "one",
				Category = "Ravens",
				Tags = new Tag[]{
					 new Tag(){ Name = "birds" }
				},
			};

			db.Put("blogOne", null, RavenJObject.FromObject(blogOne), new RavenJObject(), null);

			var results = db.ExecuteDynamicQuery(null, new IndexQuery()
			{
				PageSize = 128,
				Start = 0,
				Cutoff = SystemTime.UtcNow,
				Query = "Tags,Name:[[birds]]",
				FieldsToFetch = new string[] { "Title", "Category" }
			});

			Assert.Equal(1, results.Results.Count);
			Assert.Equal("one", results.Results[0].Value<string>("Title"));
			Assert.Equal("Ravens", results.Results[0].Value<string>("Category"));
		}

		public class Blog
		{
			public User User
			{
				get;
				set;
			}

			public string Title
			{
				get;
				set;
			}

			public Tag[] Tags
			{
				get;
				set;
			}

			public string Category
			{
				get;
				set;
			}
		}

		public class Tag
		{
			public string Name
			{
				get;
				set;
			}
		}

		public class User
		{
			public string Name
			{
				get;
				set;
			}
		}
	}
}
