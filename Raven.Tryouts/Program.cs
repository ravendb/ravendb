using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using GoogleCode.Data;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database.Config;
using Raven.Tests.Indexes;
using Raven.Tests.MailingList;

namespace BulkStressTest
{
	class Program
	{
		private const string DbName = "BulkStressTestDb";
		static void Main()
		{
			Console.WriteLine("start");
			foreach (var process in Process.GetProcessesByName("fiddler"))
			{
				Console.WriteLine(process.ProcessName);
			}
			Console.WriteLine("end");
		}

		private static void BulkInsert(int numberOfItems, IndexInfo useIndexes, BulkInsertOptions bulkInsertOptions = null)
		{
			Console.WriteLine("Starting Bulk insert for {0:#,#}", numberOfItems);
			switch (useIndexes)
			{
				case IndexInfo.DontIndex:
					Console.WriteLine("Not using indexes");
					break;
				case IndexInfo.IndexAfter:
					Console.WriteLine("Put indexes after all items are Added");
					break;
				case IndexInfo.IndexBefore:
					Console.WriteLine("Put indexes before all items are added");
					break;
				case IndexInfo.AlreadyAdded:
					Console.WriteLine("Indexes Already Added");
					break;
			}

			if (bulkInsertOptions != null && bulkInsertOptions.CheckForUpdates)
				Console.WriteLine("Using check for updates");
			else
				Console.WriteLine("Not using check for updates");

			using (var store = new DocumentStore
								   {
									   Url = "http://localhost:8080",
									   DefaultDatabase = DbName
								   }.Initialize())
			using (var bulkInsert = store.BulkInsert(DbName, bulkInsertOptions))
			{
				var stopWatch = new Stopwatch();

				if (useIndexes == IndexInfo.IndexBefore)
					AddIndexesToDatabase(store);

				stopWatch.Start();
				for (var i = 0; i < numberOfItems; i++)
				{
					bulkInsert.Store(new BlogPost
										 {
											 Id = "blogposts/" + i,
											 Author = "Author" + i % 20,
											 Category = "Category" + i % 3,
											 Content = "This is the content for blog post " + i,
											 Title = "Blog post " + i,
											 Comments = new[]{new BlogComment
												                  {
													                  Commenter = string.Format("user/{0}", i % 50),
																	  Content = "This is a comment",
																	  Title = "Commenting on blogpost/" + i
												                  } },
											 Tags = new[] { string.Format("Tag{0}", i % 10), string.Format("Tag{0}", i % 5) }
										 });
				}

				int count = 0;
				bulkInsert.Report += s =>
										 {
											 if (count++ % 10 == 0)
											 {
												 Console.Write("\r" + s);
											 }
											 if (s.StartsWith("Done ", StringComparison.InvariantCultureIgnoreCase))
											 {
												 //stopWatch.Stop();
												 Console.WriteLine("Operation took: " + stopWatch.Elapsed);

												 if (useIndexes == IndexInfo.IndexAfter)
													 AddIndexesToDatabase(store);

												 if (useIndexes != IndexInfo.DontIndex)
												 {
													 while (true)
													 {
														 var query1 = store.DatabaseCommands.Query("BlogPosts/PostsCountByTag", new IndexQuery(), new string[0]);
														 var query2 = store.DatabaseCommands.Query("BlogPosts/CountByCommenter", new IndexQuery(), new string[0]);
														 var query3 = store.DatabaseCommands.Query("SingleMapIndex", new IndexQuery(), new string[0]);
														 var query4 = store.DatabaseCommands.Query("SingleMapIndex2", new IndexQuery(), new string[0]);
														 var query5 = store.DatabaseCommands.Query("BlogPost/Search", new IndexQuery(), new string[0]);

														 if (query1.IsStale || query2.IsStale || query3.IsStale || query4.IsStale || query5.IsStale)
														 {
															 Thread.Sleep(100);
														 }
														 else
														 {
															 Console.WriteLine("Indexing finished took: " + stopWatch.Elapsed);
															 return;
														 }
													 }
												 }
											 }
										 };
			}
		}

		private static void AddIndexesToDatabase(IDocumentStore store)
		{
			IndexCreation.CreateIndexes(typeof(BlogPosts_PostsCountByTag).Assembly, store);
		}
	}

	public enum IndexInfo
	{
		DontIndex,
		IndexBefore,
		IndexAfter,
		AlreadyAdded
	}

	public class BlogPost
	{
		public string Id { get; set; }
		public string Title { get; set; }
		public string Category { get; set; }
		public string Content { get; set; }
		public DateTime PublishedAt { get; set; }
		public string[] Tags { get; set; }
		public BlogComment[] Comments { get; set; }
		public string Author { get; set; }
	}

	public class BlogComment
	{
		public string Title { get; set; }
		public string Content { get; set; }
		public string Commenter { get; set; }
		public DateTime At { get; set; }

		public BlogComment()
		{
			At = DateTime.Now;
		}
	}

	public class BlogAuthor
	{
		public string Name { get; set; }
		public string ImageUrl { get; set; }
	}

	public class BlogPosts_PostsCountByTag : AbstractIndexCreationTask<BlogPost, BlogPosts_PostsCountByTag.ReduceResult>
	{
		public class ReduceResult
		{
			public string Tag { get; set; }
			public int Count { get; set; }
		}

		public BlogPosts_PostsCountByTag()
		{
			Map = posts => from post in posts
						   from tag in post.Tags
						   select new
						   {
							   Tag = tag,
							   Count = 1
						   };

			Reduce = results => from result in results
								group result by result.Tag
									into g
									select new
									{
										Tag = g.Key,
										Count = g.Sum(x => x.Count)
									};
		}
	}

	public class BlogPosts_CountByCommenter : AbstractIndexCreationTask<BlogPost, BlogPosts_CountByCommenter.ReduceResult>
	{
		public class ReduceResult
		{
			public string Commenter { get; set; }
			public int Count { get; set; }
		}

		public BlogPosts_CountByCommenter()
		{
			Map = posts => from post in posts
						   from comment in post.Comments
						   select new
						   {
							   Commenter = comment.Commenter,
							   Count = 1
						   };

			Reduce = results => from result in results
								group result by result.Commenter
									into g
									select new
									{
										Commenter = g.Key,
										Count = g.Sum(x => x.Count)
									};
		}
	}

	public class SingleMapIndex : AbstractIndexCreationTask<BlogPost>
	{
		public SingleMapIndex()
		{
			Map = posts => from post in posts
						   select new { post.Title };
		}
	}

	public class BlogPost_Search : AbstractIndexCreationTask<BlogPost, BlogPost_Search.ReduceResult>
	{
		public class ReduceResult
		{
			public string Query { get; set; }
			public DateTime LastCommentDate { get; set; }
		}

		public BlogPost_Search()
		{
			Map = blogposts => from blogpost in blogposts
							   select new
										  {
											  Query = new object[]
						                                  {
							                                  blogpost.Author,
							                                  blogpost.Category,
							                                  blogpost.Content,
							                                  blogpost.Comments.Select(comment => comment.Title)
						                                  },
											  LastPaymentDate = blogpost.Comments.Last().At
										  };
		}
	}

	public class SingleMapIndex2 : AbstractIndexCreationTask<BlogPost>
	{
		public SingleMapIndex2()
		{
			Map = posts => from post in posts
						   select new { post.Title };
		}
	}
}

namespace GoogleCode.Data
{
	public class Project
	{
		public string Name { get; set; }
		public string CodeLicense { get; set; }
		public string CodeUrl { get; set; }
		public string ContentLicense { get; set; }
		public string ContentUrl { get; set; }
		public string Summary { get; set; }
		public string Description { get; set; }
		public List<string> Labels { get; set; }
		public List<Blog> Blogs { get; set; }
		public List<Person> People { get; set; }
		public List<Link> Links { get; set; }
		public List<Group> Groups { get; set; }
	}

	public class Blog
	{
		public string Title { get; set; }
		public string Link { get; set; }
	}

	public class Group
	{
		public string Name { get; set; }
		public string Url { get; set; }
	}

	public class Person
	{
		public string Name { get; set; }
		public string UserId { get; set; }
		public string Role { get; set; }
	}

	public class Link
	{
		public string Title { get; set; }
		public string Url { get; set; }
	}

	public class Projects_Search : AbstractIndexCreationTask<Project, Projects_Search.Result>
	{
		public class Result
		{
			public string Query { get; set; }
		}

		public Projects_Search()
		{
			Map = projects =>
				  from p in projects
				  select new
				  {
					  Query = new[]
				      {
					      p.Summary
				      }
				  };
			Store(x => x.Query, FieldStorage.Yes);
			Index(x=>x.Query, FieldIndexing.Analyzed);
		}
	}
}