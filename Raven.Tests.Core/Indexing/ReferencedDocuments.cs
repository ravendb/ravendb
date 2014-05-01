// -----------------------------------------------------------------------
//  <copyright file="ReferencedDocuments.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Tests.Core.Utils.Entities;
using Raven.Tests.Core.Utils.Indexes;
using Xunit;

namespace Raven.Tests.Core.Indexing
{
	public class ReferencedDocuments : RavenCoreTestBase
	{
		[Fact]
		public void CanUseLoadDocumentToIndexReferencedDocs()
		{
			using (var store = GetDocumentStore())
			{
				var postsByContent = new Posts_ByContent();
				postsByContent.Execute(store);

				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 10; i++)
					{
						session.Store(new Post
						{
							Id = "posts/" + i
						});

						session.Store(new PostContent
						{
							Id = "posts/" + i + "/content",
							Text = i % 2 == 0 ? "HTML 5" : "Javascript"
						});
					}

					session.SaveChanges();
					WaitForIndexing(store);

					var html5PostsQuery = session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", "HTML 5");
					var javascriptPostsQuery = session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", "Javascript");

					Assert.Equal(5, html5PostsQuery.ToList().Count);
					Assert.Equal(5, javascriptPostsQuery.ToList().Count);
				}
			}
		}

		[Fact]
		public void ShouldReindexOnReferencedDocumentChange()
		{
			using (var store = GetDocumentStore())
			{
				var postsByContent = new Posts_ByContent();
				postsByContent.Execute(store);

				using (var session = store.OpenSession())
				{
					PostContent last = null;
					for (int i = 0; i < 3; i++)
					{
						session.Store(new Post
						{
							Id = "posts/" + i
						});

						session.Store(last = new PostContent
						{
							Id = "posts/" + i + "/content",
							Text = i % 2 == 0 ? "HTML 5" : "Javascript"
						});
					}

					session.SaveChanges();
					WaitForIndexing(store);

					Assert.Equal(2, session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", "HTML 5").ToList().Count);
					Assert.Equal(1, session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", "Javascript").ToList().Count);

					last.Text = "JSON"; // referenced document change

					session.Store(last);

					session.SaveChanges();
					WaitForIndexing(store);

					Assert.Equal(1, session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", "HTML 5").ToList().Count);
					Assert.Equal(1, session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", "Javascript").ToList().Count);
					Assert.Equal(1, session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", "JSON").ToList().Count);

					session.Delete(last); // referenced document delete

					session.SaveChanges();
					WaitForIndexing(store);

					Assert.Equal(0, session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", "JSON").ToList().Count);
				}
			}
		}

		[Fact]
		public void CanProceedWhenReferencedDocumentsAreMissing()
		{
			using (var store = GetDocumentStore())
			{
				var postsByContent = new Posts_ByContent();
				postsByContent.Execute(store);

				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 10; i++)
					{
						session.Store(new Post
						{
							Id = "posts/" + i
						});

						if (i % 2 == 0)
						{
							session.Store(new PostContent
							{
								Id = "posts/" + i + "/content",
								Text = "HTML 5"
							});
						}
					}

					session.SaveChanges();
					WaitForIndexing(store);

					Assert.Equal(5, session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", null).ToList().Count);
				}
			}
		}
	}
}