// -----------------------------------------------------------------------
//  <copyright file="Oguzhntopcu.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;
using Raven.Client.Linq;

namespace Raven.Tests.MailingList
{
	public class Oguzhntopcu : RavenTest
	{
		internal class Result
		{
			public string Id { get; set; }
			public string UserName { get; set; }
			public string Password { get; set; }
			public string SenderId { get; set; }
			public PostStatus[] PostStatus { get; set; }
			public string Title { get; set; }
			public string Body { get; set; }
		}

		internal class Transformed
		{
			public string Id { get; set; }
			public string UserName { get; set; }
			public string Password { get; set; }
			public string SenderId { get; set; }
			public PostStatus[] PostStatus { get; set; }
			public string Title { get; set; }
			public string Body { get; set; }
		}

		internal class SearchIndex : AbstractMultiMapIndexCreationTask<Result>
		{
			public SearchIndex()
			{
				AddMap<Post>(items => from x in items
									  select new
									  {
										  x.Id,
										  UserName = (string)null,
										  Password = (string)null,
										  x.SenderId,
										  x.Title,
										  x.Body,
										  PostStatus = x.PostStatuses
									  });

				AddMap<User>(items => from x in items
									  from post in x.ReducedPosts
									  select new
									  {
										  post.Id,
										  x.UserName,
										  x.Password,
										  SenderId = x.Id,
										  Title = (string)null,
										  Body = (string)null,
										  PostStatus = new[] { PostStatus.None },
									  });

				Reduce = results => from x in results
									group x by x.Id
										into g
										select new
										{
											Id = g.Key,
											g.FirstOrDefault(i => i.UserName != null).UserName,
											g.FirstOrDefault(i => i.Password != null).Password,
											g.FirstOrDefault(i => i.SenderId != null).SenderId,
											g.FirstOrDefault(i => i.Title != null).Title,
											g.FirstOrDefault(i => i.Body != null).Body,
											PostStatus = g.SelectMany(i => i.PostStatus).Where(i => i != PostStatus.None).ToArray(),
										};

				TransformResults = (database, results) => from x in results
														  select new
														  {
															  x.Id,
															  x.UserName,
															  x.Password,
															  x.SenderId,
															  x.Title,
															  x.Body,
															  x.PostStatus
														  };

				Index(i => i.UserName, FieldIndexing.Analyzed);
			}
		}

		internal class User
		{
			public string Id { get; set; }
			public string UserName { get; set; }
			public string Password { get; set; }

			public ReducedPost[] ReducedPosts { get; set; }
		}

		internal class Post
		{
			public string Id { get; set; }
			public string SenderId { get; set; }
			public PostStatus[] PostStatuses { get; set; }

			public string Title { get; set; }

			public string Body { get; set; }
		}

		internal class ReducedPost
		{
			public string Id { get; set; }
		}

		public enum PostStatus : byte
		{
			None = 0,
			Ok = 1,
			Edited = 2,
			Suspended = 4,
			Deleted = 8,
		}

		public Oguzhntopcu()
		{
			DocumentStore = new EmbeddableDocumentStore
			{
				RunInMemory = true,
				Conventions =
				{
					SaveEnumsAsIntegers = true
				}
			};
			DocumentStore.Initialize();
			DocumentStore.ExecuteIndex(new SearchIndex());

			PopulateData();
		}

		public EmbeddableDocumentStore DocumentStore { get; set; }

		public IEnumerable<object> FakeDatas
		{
			get
			{
				yield return new User
				{
					Id = "user/1",
					Password = "pass",
					UserName = "user",
					ReducedPosts = new[]
                                                        {
                                                            new ReducedPost {Id = "post/1"},
                                                            new ReducedPost {Id = "post/2"}
                                                        },
				};

				yield return new Post
				{
					Id = "post/1",
					SenderId = "user/1",
					Body = "body",
					Title = "title",
					PostStatuses = new[] { PostStatus.Deleted, PostStatus.Edited }
				};

				yield return new Post
				{
					Id = "post/2",
					SenderId = "user/1",
					Body = "body 2",
					Title = "title 2",
					PostStatuses = new[] { PostStatus.Ok }
				};
			}
		}

		[Fact]
		public void CanQueryOnFlagArrays()
		{
			using (var session = DocumentStore.OpenSession())
			{
				var query = session.Query<Result, SearchIndex>()
					.Customize(i => i.WaitForNonStaleResults())
													   .Where(i => i.PostStatus.Equals(PostStatus.Edited))
													   .As<Transformed>();

				var data = query.FirstOrDefault();
				Assert.Empty(DocumentStore.DocumentDatabase.Statistics.Errors);
				Assert.NotNull(data);
				Assert.NotNull(data.PostStatus);
				//Assert.Contains(PostStatus.Edited, data.PostStatus);
			}
		}

		public void PopulateData()
		{
			using (var session = DocumentStore.OpenSession())
			{
				FakeDatas.ToList().ForEach(session.Store);

				session.SaveChanges();
			}
		}

		public override void Dispose()
		{
			DocumentStore.Dispose();
			base.Dispose();
		}
	}
}