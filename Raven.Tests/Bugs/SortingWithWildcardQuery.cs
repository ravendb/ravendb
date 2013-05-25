//-----------------------------------------------------------------------
// <copyright file="SortingWithWildcardQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Server;
using Raven.Tests.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class SortingWithWildcardQuery : RemoteClientTest, IDisposable
	{
		private readonly string path;
		private readonly RavenDbServer ravenDbServer;
		private readonly IDocumentStore documentStore;

		public SortingWithWildcardQuery()
		{
			const int port = 8079;
			path = GetPath("TestDb");
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8079);

			ravenDbServer = GetNewServer(port, path);
			documentStore = new DocumentStore {Url = "http://localhost:" + port}.Initialize();
		}


		public override void Dispose()
		{
			documentStore.Dispose();
			ravenDbServer.Dispose();
			IOExtensions.DeleteDirectory(path);
			base.Dispose();
		}

		[Fact]
		public void Can_sort_using_a_index()
		{
			documentStore.DatabaseCommands.PutIndex("CompaniesByName", new
			                                                           	IndexDefinition
			                                                           	{
			                                                           		Map = "from company in docs.Companies select new {company.Name, NameForSorting = company.Name}",
			                                                           		SortOptions = {{"NameForSorting", SortOptions.String}},
			                                                           		Indexes =
			                                                           			{
			                                                           				{"NameForSorting", FieldIndexing.NotAnalyzed},
			                                                           				{"Name", FieldIndexing.Analyzed},
			                                                           			}
			                                                           	});

			using (var session = documentStore.OpenSession())
			{
				session.Store(new Company {Name = "Nunc interdum volutpat"});
				session.Store(new Company {Name = "Fusce vulputate tempor"});
				session.Store(new Company {Name = "Quisque vulputate eros"});
				session.Store(new Company {Name = "Fusce vulputate lobortis"});
				session.Store(new Company {Name = "Nunc volutpat malesuada"});
				session.SaveChanges();

				session.Advanced.LuceneQuery<Company>("CompaniesByName").WaitForNonStaleResults().ToArray();
				// wait for the index to settle down
			}

			using (var session = documentStore.OpenSession())
			{
				var q = session
					.Advanced.LuceneQuery<Company>("CompaniesByName")
					.OrderBy("NameForSorting")
					.ToArray();

				Assert.Equal("Fusce vulputate lobortis", q[0].Name);
				Assert.Equal("Fusce vulputate tempor", q[1].Name);
				Assert.Equal("Nunc interdum volutpat", q[2].Name);
				Assert.Equal("Nunc volutpat malesuada", q[3].Name);
				Assert.Equal("Quisque vulputate eros", q[4].Name);

				q = session
					.Advanced.LuceneQuery<Company>("CompaniesByName")
					.Where("Name:vul*")
					.OrderBy("NameForSorting")
					.Take(3)
					.ToArray();

				Assert.Equal(3, q.Count());
				Assert.Equal("Fusce vulputate lobortis", q[0].Name);
				Assert.Equal("Fusce vulputate tempor", q[1].Name);
				Assert.Equal("Quisque vulputate eros", q[2].Name);
			}
		}
	}
}