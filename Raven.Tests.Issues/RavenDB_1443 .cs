// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1443 .cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1443 : RavenTest
	{
		public class Article
		{
			public string Name { get; set; }
			public string ArticleBody { get; set; }
		}

		public class ArticleIndex : AbstractIndexCreationTask<Article>
		{
			public ArticleIndex()
			{
				Map = docs => from doc in docs
							  select new
							  {
								  doc.ArticleBody
							  };

				Stores = new Dictionary<Expression<Func<Article, object>>, FieldStorage>
                         {
                             {
                                 x => x.ArticleBody, FieldStorage.Yes
                             }
                         };
			}
		}

		[Fact]
		public void CanUseMoreLikeThisLazy_Remote()
		{
			using (var store = NewRemoteDocumentStore())
			{
				ActualTestCase(store);
			}
		}

		[Fact]
		public void CanUseMoreLikeThisLazy_Embedded()
		{
			using (var store = NewDocumentStore())
			{
				ActualTestCase(store);
			}
		}

		private static void ActualTestCase(IDocumentStore store)
		{
			var lorem = new[]
			{
				"Lorem ipsum dolor sit amet, consectetur adipiscing elit. Etiam semper, leo sit amet auctor aliquam, erat ligula dictum eros",
				" Aliquam eleifend, dui vitae fermentum bibendum, nunc sem tempus risus, posuere interdum arcu diam sit amet sem",
				" Suspendisse a nunc rutrum, rutrum arcu ut, tempor est."
			};

			var index = new ArticleIndex();
			store.ExecuteIndex(index);

			using (var session = store.OpenSession())
			{
				for (int i = 0; i < 10; i++)
				{
					var article = new Article()
					{
						Name = "Art/" + i,
						ArticleBody = lorem[i%3]
					};
					session.Store(article, "articles/" + i);
				}

				session.SaveChanges(); 
			}

			WaitForIndexing(store);

			using (var session = store.OpenSession())
			{
				var oldRequests = session.Advanced.NumberOfRequests;

				var moreLikeThisLazy = session.Advanced.Lazily.MoreLikeThis<Article>(new MoreLikeThisQuery()
				{
					IndexName = index.IndexName,
					DocumentId = "articles/0",
					MinimumTermFrequency = 0,
					MinimumDocumentFrequency = 0,
				});

				Assert.Equal(oldRequests, session.Advanced.NumberOfRequests);
				Assert.NotEmpty(moreLikeThisLazy.Value);

				Assert.Equal(oldRequests + 1, session.Advanced.NumberOfRequests);
			}
		}
	}
}