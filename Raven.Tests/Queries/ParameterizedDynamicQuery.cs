//-----------------------------------------------------------------------
// <copyright file="ParameterizedDynamicQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Linq;
using System.Threading;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Client.Embedded;
using Raven.Database.Config;
using Raven.Database.Queries;
using Raven.Tests.Common;
using Raven.Tests.Storage;
using Xunit;
using Raven.Database;
using Raven.Json.Linq;

namespace Raven.Tests.Queries
{
	public class ParameterizedDynamicQuery : RavenTest
	{
		private readonly EmbeddableDocumentStore store;
		private readonly DocumentDatabase db;

		public ParameterizedDynamicQuery()
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

			db.Documents.Put("blogOne", null, RavenJObject.FromObject(blogOne), new RavenJObject(), null);
			db.Documents.Put("blogTwo", null, RavenJObject.FromObject(blogTwo), new RavenJObject(), null);
			db.Documents.Put("blogThree", null, RavenJObject.FromObject(blogThree), new RavenJObject(), null);

			var results = db.ExecuteDynamicQuery(null,new IndexQuery()
		   {
			   PageSize = 128,
			   Start = 0,
			   Cutoff = SystemTime.UtcNow,
			   Query = "Title.Length:3 AND Category:Rhinos"
           }, CancellationToken.None);

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

			db.Documents.Put("blogOne", null, RavenJObject.FromObject(blogOne), new RavenJObject(), null);
			db.Documents.Put("blogTwo", null, RavenJObject.FromObject(blogTwo), new RavenJObject(), null);
			db.Documents.Put("blogThree", null, RavenJObject.FromObject(blogThree), new RavenJObject(), null);

			int initialIndexCount = db.Statistics.CountOfIndexes;
			db.ExecuteDynamicQuery(null,new IndexQuery()
			{
				PageSize = 128,
				Start = 0,
				Cutoff = SystemTime.UtcNow,
				Query = "Title.Length:3 AND Category:Rhinos"
            }, CancellationToken.None);
			db.ExecuteDynamicQuery(null, new IndexQuery()
			{
				PageSize = 128,
				Start = 0,
				Cutoff = SystemTime.UtcNow,
				Query = "Title.Length:3 AND Category:Rhinos"
            }, CancellationToken.None);
			db.ExecuteDynamicQuery(null, new IndexQuery()
			{
				PageSize = 128,
				Start = 0,
				Cutoff = SystemTime.UtcNow,
				Query = "Category:Rhinos AND Title.Length:3"
            }, CancellationToken.None);

			Assert.True(db.Statistics.CountOfIndexes == initialIndexCount + 1);
						
		}

		[Fact]
		public void SingleInvokedQueryShouldCreateAutoIndex()
		{
			int initialIndexCount = db.Statistics.CountOfIndexes;


			db.ExecuteDynamicQuery(null, new IndexQuery()
				{
					PageSize = 128,
					Start = 0,
					Cutoff = SystemTime.UtcNow,
					Query = "Title.Length:3 AND Category:Rhinos"
                }, CancellationToken.None);
		  

			var autoIndexName = db.IndexDefinitionStorage.IndexNames.Where(x => x.StartsWith("Auto")).SingleOrDefault();
			Assert.False(string.IsNullOrEmpty(autoIndexName));
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

			db.Documents.Put("blogOne", null, RavenJObject.FromObject(blogOne), new RavenJObject(), null);
			db.Documents.Put("blogTwo", null, RavenJObject.FromObject(blogTwo), new RavenJObject(), null);
			db.Documents.Put("blogThree", null, RavenJObject.FromObject(blogThree), new RavenJObject(), null);

			QueryResult results = null;

			results = db.ExecuteDynamicQuery(null, new IndexQuery()
				{
					PageSize = 128,
					Start = 0,
					Cutoff = SystemTime.UtcNow,
					Query = "Title.Length_Range:[0x00000004 TO 0x00000009]"
                }, CancellationToken.None);

			Assert.Equal(1, results.TotalResults);
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

			db.Documents.Put("blogOne", null, RavenJObject.FromObject(blogOne), new RavenJObject(), null);
			db.Documents.Put("blogTwo", null, RavenJObject.FromObject(blogTwo), new RavenJObject(), null);
			db.Documents.Put("blogThree", null, RavenJObject.FromObject(blogThree), new RavenJObject(), null);

			var results = db.ExecuteDynamicQuery(null, new IndexQuery()
			{
				PageSize = 128,
				Start = 0,
				Cutoff = SystemTime.UtcNow,
				Query = "Tags,Name:[[birds]]"
            }, CancellationToken.None);

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

			db.Documents.Put("blogOne", null, RavenJObject.FromObject(blogOne), new RavenJObject(), null);
			db.Documents.Put("blogTwo", null, RavenJObject.FromObject(blogTwo), new RavenJObject(), null);
			db.Documents.Put("blogThree", null, RavenJObject.FromObject(blogThree), new RavenJObject(), null);

			var results = db.ExecuteDynamicQuery(null, new IndexQuery()
			{
				PageSize = 128,
				Start = 0,
				Cutoff = SystemTime.UtcNow,
				Query = "User.Name:rob"
            }, CancellationToken.None);

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

			db.Documents.Put("blogOne", null, RavenJObject.FromObject(blogOne), new RavenJObject(), null);

			var results = db.ExecuteDynamicQuery(null, new IndexQuery()
			{
				PageSize = 128,
				Start = 0,
				Cutoff = SystemTime.UtcNow,
				Query = "Tags,Name:[[birds]]",
				FieldsToFetch = new string[] { "Title", "Category" }
            }, CancellationToken.None);

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
