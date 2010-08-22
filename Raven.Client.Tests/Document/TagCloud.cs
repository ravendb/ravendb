using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Raven.Client.Document;
using Raven.Database.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Client.Tests.Document
{
	public class TagCloud : RemoteClientTest, IDisposable
	{
		private string path;

		#region IDisposable Members

		public void Dispose()
		{
			Directory.Delete(path, true);
		}

		#endregion


		private DocumentStore NewDocumentStore()
		{
			path = Path.GetDirectoryName(Assembly.GetAssembly(typeof(DocumentStoreServerTests)).CodeBase);
			path = Path.Combine(path, "TestDb").Substring(6);
			var documentStore = new DocumentStore
			{
				Configuration =
					{
						RunInUnreliableYetFastModeThatIsNotSuitableForProduction =true,
						DataDirectory = path
					}
			};
			documentStore.Initialize();
			return documentStore;
		}

		[Fact]
		public void CanQueryMapReduceIndex()
		{
			using(var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("TagCloud",
				                                new IndexDefinition
				                                {
				                                	Map =
				                                		@"
from post in docs.Posts 
from Tag in post.Tags
select new { Tag, Count = 1 }",

				                                	Reduce =
				                                		@"
from result in results
group result by result.Tag into g
select new { Tag = g.Key, Count = g.Sum(x => (long)x.Count) }"
				                                });

				using(var session = store.OpenSession())
				{
					session.Store(new Post
					{
						PostedAt = DateTime.Now,
						Tags = new List<string>{"C#", "Programming","NoSql"}
					});
					session.Store(new Post
					{
						PostedAt = DateTime.Now,
						Tags = new List<string> { "Database", "NoSql" }
					});
					session.SaveChanges();

                    var tagAndCounts = session.LuceneQuery<TagAndCount>("TagCloud").WaitForNonStaleResults()
						.ToArray();

					Assert.Equal(1, tagAndCounts.First(x=>x.Tag == "C#").Count);
					Assert.Equal(1, tagAndCounts.First(x => x.Tag == "Database").Count);
					Assert.Equal(2, tagAndCounts.First(x => x.Tag == "NoSql").Count);
					Assert.Equal(1, tagAndCounts.First(x => x.Tag == "Programming").Count);
				}
			}
		}

		public void CanStoreAndRetrieveTime(DateTime expectedTime)
		{
			//  Only supporting accuracy up to the millisecond
			expectedTime = new DateTime(expectedTime.Year, expectedTime.Month, expectedTime.Day, expectedTime.Hour, expectedTime.Minute, expectedTime.Second, expectedTime.Millisecond, expectedTime.Kind);

			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Post
					{
						PostedAt = expectedTime,
						Tags = new List<string> { "C#", "Programming", "NoSql" }
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var posts = session.Query<Post>("Raven/DocumentsByEntityName")
						.Customize(q => q.WaitForNonStaleResultsAsOfNow(TimeSpan.FromSeconds(5)))
						.ToArray();

					Assert.Equal(1, posts.Length);
					Assert.Equal(expectedTime, posts[0].PostedAt);
				}
			}
		}

		[Fact]
		public void CanStoreAndRetrieveTimeLocal()
		{
			CanStoreAndRetrieveTime(DateTime.Now);
		}

		[Fact]
		public void CanStoreAndRetrieveTimeUtc()
		{
			CanStoreAndRetrieveTime(DateTime.UtcNow);
		}

		[Fact]
		public void CanQueryMapReduceIndex_WithUpdates()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("TagCloud",
												new IndexDefinition
												{
													Map =
														@"
from post in docs.Posts 
from Tag in post.Tags
select new { Tag, Count = 1 }",

													Reduce =
														@"
from result in results
group result by result.Tag into g
select new { Tag = g.Key, Count = g.Sum(x => (long)x.Count) }"
												});

				using (var session = store.OpenSession())
				{
					session.Store(new Post
					{
						PostedAt = DateTime.Now,
						Tags = new List<string> { "C#", "Programming", "NoSql" }
					});
					session.Store(new Post
					{
						PostedAt = DateTime.Now,
						Tags = new List<string> { "Database", "NoSql" }
					});
					session.SaveChanges();

                    var tagAndCounts = session.LuceneQuery<TagAndCount>("TagCloud").WaitForNonStaleResults()
					.ToArray();

					Assert.Equal(1, tagAndCounts.Single(x => x.Tag == "C#").Count);
					Assert.Equal(1, tagAndCounts.Single(x => x.Tag == "Database").Count);
					Assert.Equal(2, tagAndCounts.Single(x => x.Tag == "NoSql").Count);
					Assert.Equal(1, tagAndCounts.Single(x => x.Tag == "Programming").Count);
		
					session.Store(new Post
					{
						PostedAt = DateTime.Now,
						Tags = new List<string> { "C#", "Programming", "NoSql" }
					});
					session.Store(new Post
					{
						PostedAt = DateTime.Now,
						Tags = new List<string> { "Database", "NoSql" }
					});
					session.SaveChanges();

                    tagAndCounts = session.LuceneQuery<TagAndCount>("TagCloud").WaitForNonStaleResults()
						.ToArray();

					Assert.Equal(2, tagAndCounts.Single(x => x.Tag == "C#").Count);
					Assert.Equal(2, tagAndCounts.Single(x => x.Tag == "Database").Count);
					Assert.Equal(4, tagAndCounts.Single(x => x.Tag == "NoSql").Count);
					Assert.Equal(2, tagAndCounts.Single(x => x.Tag == "Programming").Count);
				}
			}
		}

		[Fact]
		public void CanQueryMapReduceIndexOnMultipleFields()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("EventsByActivityAndCharacterCountAmount",
												  new IndexDefinition()
				   {
					   Map = @"
from doc in docs
where doc.Activity != null
select new {
	Activity = doc.Activity,
	Character = doc.Character,
	Amount = doc.Amount
}",
					   Reduce = @"
from result in results
group result by new { result.Activity, result.Character } into g 
select new
        {
            Activity = g.Key.Activity,
            Character =  g.Key.Character,
            Amount = g.Sum(x=>(long)x.Amount)
        }"
				   });

				using (var session = store.OpenSession())
				{
					session.Store(new Event
					{
						Activity = "Reading",
						Character = "Elf",
						Amount = 5
					});
					session.Store(new Event
					{
						Activity = "Reading",
						Character = "Dwarf",
						Amount = 7
					});
					session.Store(new Event
					{
						Activity = "Reading",
						Character = "Elf",
						Amount = 10
					});
					session.SaveChanges();

                    var tagAndCounts = session.LuceneQuery<ActivityAndCharacterCountAmount>("EventsByActivityAndCharacterCountAmount")
						.WaitForNonStaleResults(TimeSpan.FromHours(1))
						.ToArray();

					Assert.Equal(15, tagAndCounts.First(x => x.Activity == "Reading" && x.Character == "Elf").Amount);
					Assert.Equal(7, tagAndCounts.First(x => x.Activity == "Reading" && x.Character == "Dwarf").Amount);
					
				}
			}
		}
		
		public class ActivityAndCharacterCountAmount
		{
			public string Activity { get; set; }
			public string Character { get; set; }
			public long Amount { get; set; }
		}

		public class TagAndCount
		{
			public string Tag { get; set; }
			public long Count { get; set; }

			public override string ToString()
			{
				return string.Format("Tag: {0}, Count: {1}", Tag, Count);
			}
		}


		public class Post
		{
			public string Id { get; set; }
			public string Title { get; set; }
			public DateTime PostedAt { get; set; }
			public List<string> Tags { get; set; }

			public string Content { get; set; }
		}

		public class Event
		{
			public string Id { get; set; }
			public string Activity { get; set;}
			public string Character{ get; set;}
			public long Amount { get; set; }

		}
	}
}