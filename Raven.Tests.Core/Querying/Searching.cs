// -----------------------------------------------------------------------
//  <copyright file="Searching.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace Raven.Tests.Core.Querying
{
	public class Searching : RavenCoreTestBase
	{
		[Fact]
		public void CanSearchByMultipleTerms()
		{
			using (var store = GetDocumentStore())
			{
				store.DatabaseCommands.PutIndex("Posts/ByTitle", new IndexDefinition
				{
					Map = "from post in docs.Posts select new { post.Title }",
					Indexes = { { "Title", FieldIndexing.Analyzed } }
				});

				using (var session = store.OpenSession())
				{
					session.Store(new Post
					{
						Title = "Querying document database"
					});

					session.Store(new Post
					{
						Title = "Introduction to RavenDB"
					});

					session.Store(new Post
					{
						Title = "NOSQL databases"
					});

					session.Store(new Post
					{
						Title = "MSSQL 2012"
					});

					session.SaveChanges();

					WaitForIndexing(store);

					var aboutRavenDBDatabase =
						session.Query<Post>("Posts/ByTitle")
							.Search(x => x.Title, "database databases RavenDB")
							.ToList();

					Assert.Equal(3, aboutRavenDBDatabase.Count);

					//TODO arek
					//var exceptRavenDB =
					//	session.Query<Post>("Posts/ByTitle")
					//		.Search(x => x.Title, "RavenDB", options: SearchOptions.Not)
					//		.ToList();

					//Assert.Equal(3, exceptRavenDB.Count);
				}
			}
		}

		[Fact]
		public void CanSearchByMultipleFields()
		{
			using (var store = GetDocumentStore())
			{
				store.DatabaseCommands.PutIndex("Posts/ByTitleAndDescription", new IndexDefinition
				{
					Map = "from post in docs.Posts select new { post.Title, post.Desc }",
					Indexes = { { "Title", FieldIndexing.Analyzed }, { "Desc", FieldIndexing.Analyzed } }
				});

				using (var session = store.OpenSession())
				{
					session.Store(new Post
					{
						Title = "RavenDB in action",
						Desc = "Querying document database"
					});

					session.Store(new Post
					{
						Title = "Introduction to NOSQL",
						Desc = "Modeling in document DB"
					});

					session.Store(new Post
					{
						Title = "MSSQL 2012"
					});

					session.SaveChanges();

					WaitForIndexing(store);

					var nosqlOrQuerying =
						session.Query<Post>("Posts/ByTitleAndDescription")
							.Search(x => x.Title, "nosql")
							.Search(x => x.Desc, "querying")
							.ToList();

					Assert.Equal(2, nosqlOrQuerying.Count);
					Assert.NotNull(nosqlOrQuerying.FirstOrDefault(x => x.Id == "posts/1"));
					Assert.NotNull(nosqlOrQuerying.FirstOrDefault(x => x.Id == "posts/2"));


					//TODO arek
					//var notNosqlOrQuerying =
					//	session.Query<Post>("Posts/ByTitleAndDescription")
					//		.Search(x => x.Title, "nosql", options: SearchOptions.Not)
					//		.Search(x => x.Desc, "querying")
					//		.ToList();


					//Assert.Equal(2, notNosqlOrQuerying.Count);
					//Assert.NotNull(notNosqlOrQuerying.FirstOrDefault(x => x.Id == "posts/1"));
					//Assert.NotNull(notNosqlOrQuerying.FirstOrDefault(x => x.Id == "posts/3"));


					var nosqlAndModeling =
						session.Query<Post>("Posts/ByTitleAndDescription")
							.Search(x => x.Title, "nosql")
							.Search(x => x.Desc, "modeling", options: SearchOptions.And)
							.ToList();

					Assert.Equal(1, nosqlAndModeling.Count);
					Assert.NotNull(nosqlAndModeling.FirstOrDefault(x => x.Id == "posts/2"));
				}
			}
		}
	}
}