// -----------------------------------------------------------------------
//  <copyright file="RavenDB_302.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Bugs;
using Xunit;
using System.Linq;

namespace Raven.Tests.Issues
{
	public class RavenDB_302 : RavenTest
	{
		[Fact]
		public void CanQueryUsingDefaultField()
		{
			using(var s = NewDocumentStore())
			{
				using (var session = s.OpenSession())
				{
					session.Store(new Item{Version = "first"});
					session.Store(new Item { Version = "second" });
					session.SaveChanges();
				}
				using(var session = s.OpenSession())
				{
					var x = session.Advanced.LuceneQuery<Item>()
						.WaitForNonStaleResults()
						.UsingDefaultField("Version")
						.Where("First OR Second")
						.ToList();

					Assert.Equal(2, x.Count);
				}
			}
		}

		public class Node
		{
			public string FirstName { get; set; }
			public string LastName { get; set; }
		}

		public class Index : AbstractIndexCreationTask<Node>
		{
			public Index()
			{
				Map = nodes =>
				      from node in nodes
				      select new
				      {
						  node.LastName,
						  Query = new[]{node.FirstName, node.LastName}
				      };
			}
		}

		[Fact]
		public void CanQueryUsingDefaultField_StaticIndex()
		{
			using (var s = NewDocumentStore())
			{
				new Index().Execute(s);

				using (var session = s.OpenSession())
				{
					session.Store(new Node { FirstName= "jonas", LastName = "brown"});
					session.Store(new Node { FirstName = "arik", LastName = "smith"});
					session.SaveChanges();
				}
				using (var session = s.OpenSession())
				{
					var x = session.Advanced.LuceneQuery<Node, Index>()
						.WaitForNonStaleResults()
						.UsingDefaultField("Query")
						.Where("jonas OR smith")
						.ToList();

					Assert.Equal(2, x.Count);
				}
			}
		}

		[Fact]
		public void CanQueryUsingDefaultField_Facets()
		{
			using (var s = NewDocumentStore())
			{
				new Index().Execute(s);

				using (var session = s.OpenSession())
				{
					session.Store(new Node { FirstName = "jonas", LastName = "brown" });
					session.Store(new Node { FirstName = "arik", LastName = "smith" });
					session.Store(new FacetSetup
					{
						Id = "Raven/Facets/LastName",
						Facets =
						{
							new Facet
							{
								Mode = FacetMode.Default,
								Name = "LastName"
							}
						}
					});
					session.SaveChanges();
				}
				using (var session = s.OpenSession())
				{
					var x = session.Advanced.LuceneQuery<Node, Index>()
						.WaitForNonStaleResults()
						.UsingDefaultField("Query")
						.Where("jonas");

					GC.KeepAlive(x.ToList());// wait for the index to complete

					var ravenfacets = s.DatabaseCommands.GetFacets("Index",
						new IndexQuery { Query = x.ToString(), DefaultField = "Query" },
						"Raven/Facets/LastName");

					Assert.Equal(1, ravenfacets.Results["LastName"].Values.First(y=>y.Range == "brown").Hits);
				}
			}
		}

		[Fact]
		public void CanQueryUsingDefaultField_Remote()
		{
			using(GetNewServer())
			using (var s = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				s.DatabaseCommands.PutIndex("items_by_ver", new IndexDefinition
				{
					Map = "from doc in docs.Items select new { doc.Version }"
				});
				using (var session = s.OpenSession())
				{
					session.Store(new Item { Version = "first" });
					session.Store(new Item { Version = "second" });
					session.SaveChanges();
				}
				using (var session = s.OpenSession())
				{
					var x = session.Advanced.LuceneQuery<Item>("items_by_ver")
						.WaitForNonStaleResults()
						.UsingDefaultField("Version")
						.Where("First OR Second")
						.ToList();

					Assert.Equal(2, x.Count);
				}
			}
		}
	}
}