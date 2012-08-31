//-----------------------------------------------------------------------
// <copyright file="DocumentStoreServerTests.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using System.Transactions;
using Raven.Abstractions;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Database.Server;
using Raven.Json.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database.Extensions;
using Raven.Server;
using Raven.Tests.Indexes;
using Xunit;
using System.Linq;
using Raven.Tests.Spatial;

namespace Raven.Tests.Document
{
	public class DocumentStoreServerTests : RemoteClientTest, IDisposable
	{
		private readonly string path;
		private readonly int port;
		private readonly RavenDbServer server;
		private readonly IDocumentStore documentStore;

		public DocumentStoreServerTests()
		{
			port = 8079;
			path = GetPath("TestDb");
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8079);

			server = GetNewServer(port, path);
			documentStore = new DocumentStore {Url = "http://localhost:" + port}.Initialize();
		}

		public override void Dispose()
		{
			documentStore.Dispose();
			server.Dispose();
			IOExtensions.DeleteDirectory(path);
			base.Dispose();
		}

		[Fact]
		public void Should_insert_into_db_and_set_id()
		{
			using (var session = documentStore.OpenSession())
			{
				var entity = new Company {Name = "Company"};
				session.Store(entity);
				session.SaveChanges();
				Assert.NotEqual(Guid.Empty.ToString(), entity.Id);
			}
		}

		[Fact]
		public void Can_get_index_names()
		{
			Assert.Contains("Raven/DocumentsByEntityName", documentStore.DatabaseCommands.GetIndexNames(0, 25));
		}

		[Fact]
		public void Can_get_index_def()
		{
			documentStore.DatabaseCommands.PutIndex("Companies/Name", new IndexDefinitionBuilder<Company, Company>
			                                                          	{
			                                                          		Map = companies => from c in companies
			                                                          		                   select new {c.Name},
			                                                          		Indexes = {{x => x.Name, FieldIndexing.NotAnalyzed}}
			                                                          	});
			var indexDefinition = documentStore.DatabaseCommands.GetIndex("Companies/Name");
			Assert.Equal(@"docs.Companies.Select(c => new {
    Name = c.Name
})", indexDefinition.Map);
			Assert.Equal(FieldIndexing.NotAnalyzed, indexDefinition.Indexes["Name"]);
		}

		[Fact]
		public void Can_delete_by_index()
		{
			var entity = new Company {Name = "Company"};
			using (var session = documentStore.OpenSession())
			{
				session.Store(entity);
				session.SaveChanges();

				session.Advanced.LuceneQuery<Company>().WaitForNonStaleResults().ToArray(); // wait for the index to settle down
			}

			documentStore.DatabaseCommands.DeleteByIndex("Raven/DocumentsByEntityName", new IndexQuery
			                                                                            	{
			                                                                            		Query = "Tag:[[Companies]]"
			                                                                            	}, allowStale: false);

			using (var session = documentStore.OpenSession())
			{
				Assert.Empty(session.Advanced.LuceneQuery<Company>().WaitForNonStaleResults().ToArray());
			}
		}


		[Fact]
		public void Can_order_by_using_linq()
		{
			documentStore.DatabaseCommands.PutIndex("CompaniesByName", new IndexDefinition
			                                                           	{
			                                                           		Map = "from company in docs.Companies select new { company.Name, company.Phone }",
			                                                           	});

			using (var session = documentStore.OpenSession())
			{
				session.Store(new Company {Name = "A", Phone = 4});
				session.Store(new Company {Name = "B", Phone = 3});
				session.Store(new Company {Name = "B", Phone = 4});
				session.SaveChanges();

				session.Advanced.LuceneQuery<Company>("CompaniesByName").WaitForNonStaleResults().ToArray(); // wait for the index to settle down
			}

			using (var session = documentStore.OpenSession())
			{
				var q = from company in session.Query<Company>("CompaniesByName")
				        orderby company.Name descending
				        select company;

				var companies = q.ToArray();
				Assert.Equal("B", companies[0].Name);
				Assert.Equal("B", companies[1].Name);
				Assert.Equal("A", companies[2].Name);

				q = from company in session.Query<Company>("CompaniesByName")
				    orderby company.Name
				    select company;

				companies = q.ToArray();
				Assert.Equal("A", companies[0].Name);
				Assert.Equal("B", companies[1].Name);
				Assert.Equal("B", companies[2].Name);


				q = from company in session.Query<Company>("CompaniesByName")
				    orderby company.Phone
				    select company;

				companies = q.ToArray();
				Assert.Equal(3, companies[0].Phone);
				Assert.Equal(4, companies[1].Phone);
				Assert.Equal(4, companies[2].Phone);

				q = from company in session.Query<Company>("CompaniesByName")
				    orderby company.Phone , company.Name
				    select company;

				companies = q.ToArray();
				Assert.Equal(3, companies[0].Phone);
				Assert.Equal(4, companies[1].Phone);
				Assert.Equal(4, companies[2].Phone);
				Assert.Equal("B", companies[0].Name);
				Assert.Equal("A", companies[1].Name);
				Assert.Equal("B", companies[2].Name);
			}
		}


		[Fact]
		public void Can_create_index_with_decimal_as_firstfield()
		{
			using (var session = documentStore.OpenSession())
			{
				var company = new Company {Name = "Company 1", Phone = 5, AccountsReceivable = (decimal) 3904.39};
				session.Store(company);
				session.SaveChanges();

				documentStore.DatabaseCommands.PutIndex("company_by_name",
				                                        new IndexDefinition
				                                        	{
				                                        		Map = "from doc in docs where doc.Name != null select new { doc.AccountsReceivable, doc.Name}",
				                                        		Stores = {{"Name", FieldStorage.Yes}, {"AccountsReceivable", FieldStorage.Yes}}
				                                        	});

				var q = session.Query<Company>("company_by_name")
					.Customize(query => query.WaitForNonStaleResults(TimeSpan.FromHours(1)));
				var single = q.ToList().SingleOrDefault();

				Assert.NotNull(single);
				Assert.Equal("Company 1", single.Name);
				Assert.Equal((decimal) 3904.39, single.AccountsReceivable);
			}
		}

		[Fact]
		public void Can_select_from_index_using_linq_method_chain_using_decimal_and_greater_than_or_equal()
		{
			using (var session = documentStore.OpenSession())
			{
				var company = new Company {Name = "Company 1", Phone = 5, AccountsReceivable = (decimal) 3904.39};
				session.Store(company);
				session.SaveChanges();
				documentStore.DatabaseCommands.PutIndex("company_by_name", new
				                                                           	IndexDefinition
				                                                           	{
				                                                           		Map = "from doc in docs where doc.Name != null select new { doc.Name, doc.AccountsReceivable}",
				                                                           		Stores = {{"Name", FieldStorage.Yes}, {"AccountsReceivable", FieldStorage.Yes}}
				                                                           	});
				var q = session.Query<Company>("company_by_name")
					.Customize(query => query.WaitForNonStaleResults(TimeSpan.FromHours(1)))
					.Where(x => x.AccountsReceivable > 1);
				var single = q.ToList().SingleOrDefault();
				Assert.NotNull(single);
				Assert.Equal("Company 1", single.Name);
				Assert.Equal((decimal) 3904.39, single.AccountsReceivable);
			}
		}

		[Fact]
		public void Can_create_index_with_decimal_as_lastfield()
		{
			using (var session = documentStore.OpenSession())
			{
				var company = new Company {Name = "Company 1", Phone = 5, AccountsReceivable = (decimal) 3904.39};
				session.Store(company);
				session.SaveChanges();

				documentStore.DatabaseCommands.PutIndex("company_by_name", new IndexDefinition
				                                                           	{
				                                                           		Map = "from doc in docs where doc.Name != null select new {  doc.Name, doc.AccountsReceivable }",
				                                                           		Stores = {{"Name", FieldStorage.Yes}, {"AccountsReceivable", FieldStorage.Yes}}
				                                                           	});

				var q = session.Query<Company>("company_by_name")
					.Customize(query => query.WaitForNonStaleResults(TimeSpan.FromHours(1)));
				var single = q.ToList().SingleOrDefault();

				Assert.NotNull(single);
				Assert.Equal("Company 1", single.Name);
				Assert.Equal((decimal) 3904.39, single.AccountsReceivable);
			}
		}

		[Fact]
		public void Can_update_by_index()
		{
			var entity = new Company {Name = "Company"};
			using (var session = documentStore.OpenSession())
			{
				session.Store(entity);
				session.SaveChanges();

				session.Advanced.LuceneQuery<Company>().WaitForNonStaleResults().ToArray(); // wait for the index to settle down
			}

			documentStore.DatabaseCommands.UpdateByIndex("Raven/DocumentsByEntityName", new IndexQuery
			                                                                            	{
			                                                                            		Query = "Tag:[[Companies]]"
			                                                                            	}, new[]
			                                                                            	   	{
			                                                                            	   		new PatchRequest
			                                                                            	   			{
			                                                                            	   				Type = PatchCommandType.Set,
			                                                                            	   				Name = "Name",
			                                                                            	   				Value = RavenJToken.FromObject("Another Company")
			                                                                            	   			},
			                                                                            	   	}, allowStale: false);

			using (var session = documentStore.OpenSession())
			{
				Assert.Equal("Another Company", session.Load<Company>(entity.Id).Name);
			}
		}


		[Fact]
		public void Can_specify_cutoff_using_server()
		{
			documentStore.DatabaseCommands.Query("Raven/DocumentsByEntityName", new IndexQuery
			                                                                    	{
			                                                                    		PageSize = 10,
			                                                                    		Cutoff = SystemTime.UtcNow.AddHours(-1)
			                                                                    	}, null);
		}

		[Fact]
		public void Can_read_projected_dates()
		{
			documentStore.DatabaseCommands.PutIndex("my_index",
			                                        new IndexDefinition
			                                        	{
			                                        		Map = "from doc in docs select new { doc.Date}",
			                                        		Stores = {{"Date", FieldStorage.Yes}}
			                                        	});

			using (var s = documentStore.OpenSession())
			{
				s.Store(new
				        	{
				        		Date = new DateTime(2000, 1, 1)
				        	});
				s.SaveChanges();
			}

			using (var s = documentStore.OpenSession())
			{
				var query = s.Advanced.LuceneQuery<object>("my_index")
					.SelectFields<DateHolder>("Date")
					.WaitForNonStaleResults();
				var dateHolder = query.ToArray().First();

				Assert.Equal(new DateTime(2000, 1, 1), dateHolder.Date);
			}
		}

		public class DateHolder
		{
			public DateTime Date { get; set; }
		}

		[Fact]
		public void Can_query_using_special_characters()
		{
			documentStore.DatabaseCommands.PutIndex("my_index",
			                                        new IndexDefinition
			                                        	{
			                                        		Map = "from doc in docs select new { doc.Language, doc.Type}",
			                                        		Stores = {{"Name", FieldStorage.Yes}, {"Phone", FieldStorage.Yes}}
			                                        	});

			using (var s = documentStore.OpenSession())
			{
				s.Store(new
				        	{
				        		Language = "Fran�ais", //Note the �
				        		Type = "Feats"
				        	});
				s.SaveChanges();
			}

			using (var s = documentStore.OpenSession())
			{
				var query = s.Advanced.LuceneQuery<object>("my_index")
					.Where("Type:Feats AND Language:Fran�ais")
					.WaitForNonStaleResults();
				query.ToArray();

				Assert.Equal(1, query.QueryResult.TotalResults);
			}
		}

		[Fact]
		public void Can_query_indexes_returning_complex_objects()
		{
			documentStore.DatabaseCommands.PutIndex("my_index",
			                                        new IndexDefinition
			                                        	{
			                                        		Map = "from doc in docs select new { doc.Language, doc.Type, Value = new{ Answers = 42, Paths = 7 }  }}",
			                                        		Stores = {{"Value", FieldStorage.Yes},}
			                                        	});

			using (var s = documentStore.OpenSession())
			{
				s.Store(new
				        	{
				        		Language = "Fran�ais", //Note the �
				        		Type = "Feats"
				        	});
				s.SaveChanges();
			}

			using (var s = documentStore.OpenSession())
			{
				var query = s.Advanced.LuceneQuery<RavenJObject>("my_index")
					.Where("Type:Feats AND Language:Fran�ais")
					.SelectFields<RavenJObject>("Value")
					.WaitForNonStaleResults();
				var first = query.First();

				Assert.Equal(42, first.Value<RavenJObject>("Value").Value<int>("Answers"));
				Assert.Equal(7, first.Value<RavenJObject>("Value").Value<int>("Paths"));
			}
		}

		[Fact]
		public void Requesting_stats()
		{
			using (var session = documentStore.OpenSession())
			{
				var databaseStatistics = session.Load<DatabaseStatistics>("stats");
				Assert.Null(databaseStatistics);
			}
		}

		[Fact]
		public void Can_get_entity_back_with_enum()
		{
			var company = new Company {Name = "Company Name", Type = Company.CompanyType.Private};
			using (var session = documentStore.OpenSession())
			{
				session.Store(company);
				session.SaveChanges();
			}
			using (var session = documentStore.OpenSession())
			{
				var companyFound = session.Load<Company>(company.Id);
				Assert.Equal(companyFound.Type, company.Type);
			}
		}

		[Fact]
		public void Can_store_and_get_array_metadata()
		{
			using (var session = documentStore.OpenSession())
			{
				var company = new Company {Name = "Company"};
				session.Store(company);
				var metadata = session.Advanced.GetMetadataFor(company);
				metadata["Raven-Allowed-Users"] = new RavenJArray("ayende", "oren", "rob");

				session.SaveChanges();
				var metadataFromServer = session.Advanced.GetMetadataFor(session.Load<Company>(company.Id));
				var users = ((RavenJArray) metadataFromServer["Raven-Allowed-Users"]).Cast<RavenJValue>().Select(x => (string) x.Value).ToArray();
				Assert.Equal(new[] {"ayende", "oren", "rob"}, users);
			}
		}

		[Fact]
		public void Can_store_using_batch()
		{
			var batchResults = documentStore
				.DatabaseCommands
				.Batch(new ICommandData[]
				       	{
				       		new PutCommandData
				       			{
				       				Document = RavenJObject.FromObject(new Company {Name = "Hibernating Rhinos"}),
				       				Etag = null,
				       				Key = "rhino1",
				       				Metadata = new RavenJObject(),
				       			},
				       		new PutCommandData
				       			{
				       				Document = RavenJObject.FromObject(new Company {Name = "Hibernating Rhinos"}),
				       				Etag = null,
				       				Key = "rhino2",
				       				Metadata = new RavenJObject(),
				       			},
				       		new DeleteCommandData
				       			{
				       				Etag = null,
				       				Key = "rhino2"
				       			}
				       	});

			Assert.Equal("rhino1", batchResults[0].Key);
			Assert.Equal("rhino2", batchResults[1].Key);

			Assert.Null(documentStore.DatabaseCommands.Get("rhino2"));
			Assert.NotNull(documentStore.DatabaseCommands.Get("rhino1"));
		}

		[Fact]
		public void Can_get_document_metadata()
		{
			documentStore.DatabaseCommands
				.Put("rhino1", null, RavenJObject.FromObject(new Company {Name = "Hibernating Rhinos"}), new RavenJObject());

			JsonDocument doc = documentStore.DatabaseCommands.Get("rhino1");
			JsonDocumentMetadata meta = documentStore.DatabaseCommands.Head("rhino1");

			Assert.NotNull(meta);
			Assert.Equal(doc.Key, meta.Key);
			Assert.Equal(doc.Etag, meta.Etag);
			Assert.Equal(doc.LastModified, meta.LastModified);
		}

		[Fact]
		public void When_document_does_not_exist_Then_metadata_should_be_null()
		{
			Assert.Null(documentStore.DatabaseCommands.Head("rhino1"));
		}

		[Fact]
		public void Can_defer_commands_until_savechanges()
		{
			using (var session = documentStore.OpenSession())
			{
				var commands = new ICommandData[]
				               	{
				               		new PutCommandData
				               			{
				               				Document =
				               					RavenJObject.FromObject(new Company {Name = "Hibernating Rhinos"}),
				               				Etag = null,
				               				Key = "rhino1",
				               				Metadata = new RavenJObject(),
				               			},
				               		new PutCommandData
				               			{
				               				Document =
				               					RavenJObject.FromObject(new Company {Name = "Hibernating Rhinos"}),
				               				Etag = null,
				               				Key = "rhino2",
				               				Metadata = new RavenJObject(),
				               			}
				               	};

				session.Advanced.Defer(commands);
				session.Advanced.Defer(new DeleteCommandData
				                       	{
				                       		Etag = null,
				                       		Key = "rhino2"
				                       	});

				session.Store(new Company {Name = "Hibernating Rhinos", Id = "rhino3"});

				Assert.Equal(0, session.Advanced.NumberOfRequests);

				session.SaveChanges();
				Assert.Equal(1, session.Advanced.NumberOfRequests);

				// Make sure that session is empty
				session.SaveChanges();
				Assert.Equal(1, session.Advanced.NumberOfRequests);
			}

			Assert.Null(documentStore.DatabaseCommands.Get("rhino2"));
			Assert.NotNull(documentStore.DatabaseCommands.Get("rhino1"));
			Assert.NotNull(documentStore.DatabaseCommands.Get("rhino3"));
		}

		[Fact]
		public void Can_get_two_documents_in_one_call()
		{
			using (var session = documentStore.OpenSession())
			{
				session.Store(new Company {Name = "Company A", Id = "1"});
				session.Store(new Company {Name = "Company B", Id = "2"});
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				var companies = session.Load<Company>("1", "2");
				Assert.Equal(2, companies.Length);
				Assert.Equal("Company A", companies[0].Name);
				Assert.Equal("Company B", companies[1].Name);
			}
		}


		[Fact]
		public void Can_delete_document()
		{
			using (var session = documentStore.OpenSession())
			{
				var entity = new Company {Name = "Company"};
				session.Store(entity);
				session.SaveChanges();
				using (var session2 = documentStore.OpenSession())
					Assert.NotNull(session2.Load<Company>(entity.Id));

				session.Delete(entity);
				session.SaveChanges();

				using (var session3 = documentStore.OpenSession())
					Assert.Null(session3.Load<Company>(entity.Id));
			}
		}

		[Fact]
		public void Can_project_from_index()
		{
			using (var session = documentStore.OpenSession())
			{
				var company = new Company {Name = "Company 1", Phone = 5};
				session.Store(company);
				session.SaveChanges();

				documentStore.DatabaseCommands.PutIndex("company_by_name",
				                                        new IndexDefinition
				                                        	{
				                                        		Map = "from doc in docs where doc.Name != null select new { doc.Name, doc.Phone}",
				                                        		Stores = {{"Name", FieldStorage.Yes}, {"Phone", FieldStorage.Yes}},
				                                        		Indexes = {{"Name", FieldIndexing.NotAnalyzed}}
				                                        	});

				var q = session
					.Advanced.LuceneQuery<Company>("company_by_name")
					.SelectFields<Company>("Name", "Phone")
					.WaitForNonStaleResults();
				var single = q.Single();
				Assert.Equal("Company 1", single.Name);
				Assert.Equal(5, single.Phone);
			}
		}

		[Fact]
		public void Can_select_from_index_using_linq_method_chain()
		{
			using (var session = documentStore.OpenSession())
			{
				var company = new Company {Name = "Company 1", Phone = 5};
				session.Store(company);
				session.SaveChanges();

				documentStore.DatabaseCommands.PutIndex("company_by_name",
				                                        new IndexDefinition
				                                        	{
				                                        		Map = "from doc in docs where doc.Name != null select new { doc.Name, doc.Phone}",
				                                        		Stores = {{"Name", FieldStorage.Yes}, {"Phone", FieldStorage.Yes}}
				                                        	});

				var q = session.Query<Company>("company_by_name")
					.Customize(query => query.WaitForNonStaleResults())
					.Where(x => x.Name == (company.Name));
				var single = q.ToArray()[0];
				Assert.Equal("Company 1", single.Name);
				Assert.Equal(5, single.Phone);
			}
		}

		[Fact]
		public void Can_select_from_index_using_linq_method_chain_with_variable()
		{
			using (var session = documentStore.OpenSession())
			{
				var company = new Company {Name = "Company 1", Phone = 5};
				session.Store(company);
				session.SaveChanges();

				documentStore.DatabaseCommands.PutIndex("company_by_name",
				                                        new IndexDefinition
				                                        	{
				                                        		Map = "from doc in docs where doc.Name != null select new { doc.Name, doc.Phone}",
				                                        		Stores = {{"Name", FieldStorage.Yes}, {"Phone", FieldStorage.Yes}}
				                                        	});

				var name = company.Name;
				var q = session.Query<Company>("company_by_name")
					.Customize(query => query.WaitForNonStaleResults())
					.Where(x => x.Name == (name));
				var single = q.ToArray()[0];
				Assert.Equal("Company 1", single.Name);
				Assert.Equal(5, single.Phone);
			}
		}

		[Fact]
		public void Can_select_from_index_using_linq_with_no_where_clause()
		{
			using (var session = documentStore.OpenSession())
			{
				var company = new Company {Name = "Company 1", Phone = 5};
				session.Store(company);
				session.SaveChanges();

				documentStore.DatabaseCommands.PutIndex("company_by_name",
				                                        new IndexDefinition
				                                        	{
				                                        		Map = "from doc in docs where doc.Name != null select new { doc.Name, doc.Phone}",
				                                        		Stores = {{"Name", FieldStorage.Yes}, {"Phone", FieldStorage.Yes}}
				                                        	});

				var name = company.Name;
				var q = session.Query<Company>("company_by_name")
					.Customize(query => query.WaitForNonStaleResults());
				var single = q.ToArray()[0];
				Assert.Equal("Company 1", single.Name);
				Assert.Equal(5, single.Phone);
			}
		}

		[Fact]
		public void Can_select_from_index_using_linq_method_chain_using_integer()
		{
			using (var session = documentStore.OpenSession())
			{
				var company = new Company {Name = "Company 1", Phone = 5};
				session.Store(company);
				session.SaveChanges();

				documentStore.DatabaseCommands.PutIndex("company_by_name",
				                                        new IndexDefinition
				                                        	{
				                                        		Map = "from doc in docs where doc.Name != null select new { doc.Name, doc.Phone}",
				                                        		Stores = {{"Name", FieldStorage.Yes}, {"Phone", FieldStorage.Yes}}
				                                        	});

				var q = session.Query<Company>("company_by_name")
					.Customize(query => query.WaitForNonStaleResults())
					.Where(x => x.Phone == 5);
				var single = q.ToArray()[0];
				Assert.Equal("Company 1", single.Name);
				Assert.Equal(5, single.Phone);
			}
		}

		[Fact]
		public void Can_select_from_index_using_linq_method_chain_using_integer_and_greater_than_or_equal()
		{
			using (var session = documentStore.OpenSession())
			{
				var company = new Company {Name = "Company 1", Phone = 5};
				session.Store(company);
				session.SaveChanges();

				documentStore.DatabaseCommands.PutIndex("company_by_name",
				                                        new IndexDefinition
				                                        	{
				                                        		Map = "from doc in docs where doc.Name != null select new { doc.Name, doc.Phone}",
				                                        		Stores = {{"Name", FieldStorage.Yes}, {"Phone", FieldStorage.Yes}}
				                                        	});

				var q = session.Query<Company>("company_by_name")
					.Customize(query => query.WaitForNonStaleResults(TimeSpan.FromHours(1)))
					.Where(x => x.Phone > 1);
				var single = q.ToArray()[0];
				Assert.Equal("Company 1", single.Name);
				Assert.Equal(5, single.Phone);
			}
		}

		[Fact]
		public void Optimistic_concurrency()
		{
			using (var session = documentStore.OpenSession())
			{
				session.Advanced.UseOptimisticConcurrency = true;
				var company = new Company {Name = "Company 1"};
				session.Store(company);
				session.SaveChanges();

				using (var session2 = documentStore.OpenSession())
				{
					var company2 = session2.Load<Company>(company.Id);
					company2.Name = "foo";
					session2.SaveChanges();
				}

				company.Name = "Company 2";
				Assert.Throws<ConcurrencyException>(() => session.SaveChanges());
			}
		}

		[Fact]
		public void Can_insert_with_transaction()
		{
			const string id = "Company/id";
			using (var session = documentStore.OpenSession())
			{
				Assert.Null(session.Load<Company>(id));
				using (var tx = new TransactionScope())
				{
					var company = new Company {Id = id, Name = "Company 1"};
					session.Store(company);

					session.SaveChanges();

					tx.Complete();
				}

				Assert.NotNull(session.Load<Company>(id));
			}
		}

		[Fact]
		public void Can_rollback_transaction_on_insert()
		{
			string id;
			using (var session = documentStore.OpenSession())
			{
				using (var tx = new TransactionScope())
				{
					var company = new Company {Name = "Company 1"};
					session.Store(company);

					session.SaveChanges();

					id = company.Id;
				}
			}
			using (var session2 = documentStore.OpenSession())
			{

				Assert.Null(session2.Load<Company>(id));
			}
		}

		[Fact]
		public void Should_update_stored_entity()
		{
			using (var session = documentStore.OpenSession())
			{
				var company = new Company {Name = "Company 1"};
				session.Store(company);
				session.SaveChanges();

				var id = company.Id;
				company.Name = "Company 2";
				session.SaveChanges();

				var companyFound = session.Load<Company>(company.Id);
				Assert.Equal("Company 2", companyFound.Name);
				Assert.Equal(id, company.Id);
			}
		}

		[Fact]
		public void Should_update_retrieved_entity()
		{
			var session1 = documentStore.OpenSession();
			var company = new Company {Name = "Company 1"};
			session1.Store(company);
			session1.SaveChanges();

			var companyId = company.Id;

			var session2 = documentStore.OpenSession();
			var companyFound = session2.Load<Company>(companyId);
			companyFound.Name = "New Name";
			session2.SaveChanges();

			Assert.Equal("New Name", session2.Load<Company>(companyId).Name);
		}

		[Fact]
		public void Should_retrieve_all_entities()
		{
			var session1 = documentStore.OpenSession();
			session1.Store(new Company {Name = "Company 1"});
			session1.Store(new Company {Name = "Company 2"});

			session1.SaveChanges();

			var session2 = documentStore.OpenSession();
			var companyFound = session2.Advanced.LuceneQuery<Company>()
				.WaitForNonStaleResults()
				.ToArray();

			Assert.Equal(2, companyFound.Length);
		}

		[Fact]
		public void Should_retrieve_all_entities_using_connection_string()
		{
			var session1 = documentStore.OpenSession();
			session1.Store(new Company {Name = "Company 1"});
			session1.Store(new Company {Name = "Company 2"});

			session1.SaveChanges();

			var session2 = documentStore.OpenSession();
			var companyFound = session2.Advanced.LuceneQuery<Company>()
				.WaitForNonStaleResults()
				.ToArray();

			Assert.Equal(2, companyFound.Length);
		}

		[Fact]
		public void Can_sort_from_index()
		{
			using (var session = documentStore.OpenSession())
			{

				session.Store(new Company {Name = "Company 1", Phone = 5});
				session.Store(new Company {Name = "Company 2", Phone = 3});
				session.SaveChanges();

				documentStore.DatabaseCommands.PutIndex("company_by_name",
				                                        new IndexDefinition
				                                        	{
				                                        		Map = "from doc in docs where doc.Name != null select new { doc.Name, doc.Phone}",
				                                        		Indexes = {{"Phone", FieldIndexing.Analyzed}}
				                                        	});

				// Wait until the index is built
				session.Advanced.LuceneQuery<Company>("company_by_name")
					.WaitForNonStaleResults()
					.ToArray();

				var companies = session.Advanced.LuceneQuery<Company>("company_by_name")
					.OrderBy("Phone")
					.WaitForNonStaleResults()
					.ToArray();

				Assert.Equal("Company 2", companies[0].Name);
				Assert.Equal("Company 1", companies[1].Name);
			}
		}

		[Fact]
		public void Can_query_from_spatial_index()
		{
			using (var session = documentStore.OpenSession())
			{
				foreach (Event @event in SpatialIndexTestHelper.GetEvents())
				{
					session.Store(@event);
				}

				session.SaveChanges();

				var indexDefinition = new IndexDefinition
				                      	{
				                      		Map = "from e in docs.Events select new { Tag = \"Event\", _ = SpatialIndex.Generate(e.Latitude, e.Longitude) }",
				                      		Indexes =
				                      			{
				                      				{"Tag", FieldIndexing.NotAnalyzed}
				                      			}
				                      	};

				documentStore.DatabaseCommands.PutIndex("eventsByLatLng", indexDefinition);

				// Wait until the index is built
				session.Advanced.LuceneQuery<Event>("eventsByLatLng")
					.WaitForNonStaleResults()
					.ToArray();

				const double lat = 38.96939, lng = -77.386398;
				const double radiusInKm = 6.0 * 1.609344;

				var events = session.Advanced.LuceneQuery<Event>("eventsByLatLng")
					.WhereEquals("Tag", "Event")
					.WithinRadiusOf(radiusInKm, lat, lng)
					.SortByDistance()
					.WaitForNonStaleResults()
					.ToArray();


				var expected =
					SpatialIndexTestHelper.GetEvents()
						.Count(e => Database.Indexing.SpatialIndex.GetDistance(lat, lng, e.Latitude, e.Longitude) <= radiusInKm);

				Assert.Equal(expected, events.Length);

				Assert.Equal(7, events.Length);

				double previous = 0;
				foreach (var e in events)
				{
					double distance = Raven.Database.Indexing.SpatialIndex.GetDistance(lat, lng, e.Latitude, e.Longitude);
					Console.WriteLine("Venue: " + e.Venue + ", Distance " + distance);
					Assert.True(distance < radiusInKm);
					Assert.True(distance >= previous);
					previous = distance;
				}
			}
		}

		[Fact]
		public void Can_create_index_using_linq_from_client()
		{
			documentStore.DatabaseCommands.PutIndex("UsersByLocation", new IndexDefinitionBuilder<LinqIndexesFromClient.User>
			                                                           	{
			                                                           		Map = users => from user in users
			                                                           		               where user.Location == "Tel Aviv"
			                                                           		               select new {user.Name},
			                                                           	});

			using (var session = documentStore.OpenSession())
			{
				session.Store(new LinqIndexesFromClient.User
				              	{
				              		Location = "Tel Aviv",
				              		Name = "Yael"
				              	});

				session.SaveChanges();

				LinqIndexesFromClient.User single = session.Advanced.LuceneQuery<LinqIndexesFromClient.User>("UsersByLocation")
					.Where("Name:Yael")
					.WaitForNonStaleResults()
					.Single();

				Assert.Equal("Yael", single.Name);
			}
		}

		[Fact]
		public void Can_create_index_using_linq_from_client_using_map_reduce()
		{
			documentStore.DatabaseCommands.PutIndex("UsersCountByLocation", new IndexDefinitionBuilder<LinqIndexesFromClient.User, LinqIndexesFromClient.LocationCount>
			                                                                	{
			                                                                		Map = users => from user in users
			                                                                		               where user.Location == "Tel Aviv"
			                                                                		               select new {user.Location, Count = 1},
			                                                                		Reduce = results => from loc in results
			                                                                		                    group loc by loc.Location
			                                                                		                    into g
			                                                                		                    select new {Location = g.Key, Count = g.Sum(x => x.Count)},
			                                                                		Indexes = {{x => x.Location, FieldIndexing.NotAnalyzed}}
			                                                                	});

			using (var session = documentStore.OpenSession())
			{
				session.Store(new LinqIndexesFromClient.User
				              	{
				              		Location = "Tel Aviv",
				              		Name = "Yael"
				              	});

				session.SaveChanges();

				LinqIndexesFromClient.LocationCount single = session.Advanced.LuceneQuery<LinqIndexesFromClient.LocationCount>("UsersCountByLocation")
					.Where("Location:\"Tel Aviv\"")
					.WaitForNonStaleResults()
					.Single();

				Assert.Equal("Tel Aviv", single.Location);
				Assert.Equal(1, single.Count);
			}
		}

		[Fact]
		public void Can_get_correct_averages_from_map_reduce_index()
		{
			documentStore.DatabaseCommands.PutIndex("AvgAgeByLocation", new IndexDefinitionBuilder<LinqIndexesFromClient.User, LinqIndexesFromClient.LocationAge>
			                                                            	{
			                                                            		Map = users => from user in users
			                                                            		               select new {user.Location, user.Age},
			                                                            		Reduce = results => from loc in results
			                                                            		                    group loc by loc.Location
			                                                            		                    into g
			                                                            		                    select new {Location = g.Key, Age = g.Average(x => x.Age)},
			                                                            		Indexes = {{x => x.Location, FieldIndexing.NotAnalyzed}}
			                                                            	});

			using (var session = documentStore.OpenSession())
			{
				session.Store(new LinqIndexesFromClient.User
				              	{
				              		Location = "Tel Aviv",
				              		Age = 29,
				              		Name = "Yael"
				              	});

				session.Store(new LinqIndexesFromClient.User
				              	{
				              		Location = "Tel Aviv",
				              		Age = 24,
				              		Name = "Einat"
				              	});

				session.SaveChanges();

				LinqIndexesFromClient.LocationAge single = session.Advanced.LuceneQuery<LinqIndexesFromClient.LocationAge>("AvgAgeByLocation")
					.Where("Location:\"Tel Aviv\"")
					.WaitForNonStaleResults()
					.Single();

				Assert.Equal("Tel Aviv", single.Location);
				Assert.Equal(26.5m, single.Age);
			}
		}

		[Fact]
		public void Can_get_correct_maximum_from_map_reduce_index()
		{
			documentStore.DatabaseCommands.PutIndex("MaxAge", new IndexDefinitionBuilder<LinqIndexesFromClient.User, LinqIndexesFromClient.LocationAge>
			                                                  	{
			                                                  		Map = users => from user in users
			                                                  		               select new {user.Age},
			                                                  		Indexes = {{x => x.Age, FieldIndexing.Analyzed}},
			                                                  		Stores = {{x => x.Age, FieldStorage.Yes}}
			                                                  	});

			using (var session = documentStore.OpenSession())
			{

				session.Store(new LinqIndexesFromClient.User
				              	{
				              		Age = 27,
				              		Name = "Foo"
				              	});

				session.Store(new LinqIndexesFromClient.User
				              	{
				              		Age = 33,
				              		Name = "Bar"
				              	});

				session.Store(new LinqIndexesFromClient.User
				              	{
				              		Age = 29,
				              		Name = "Bar"
				              	});

				session.SaveChanges();

				var user = session.Advanced.LuceneQuery<LinqIndexesFromClient.User>("MaxAge")
					.OrderBy("-Age")
					.Take(1)
					.WaitForNonStaleResults()
					.Single();

				Assert.Equal(33, user.Age);
			}
		}

		[Fact]
		public void Using_attachments()
		{
			var attachment = documentStore.DatabaseCommands.GetAttachment("ayende");
			Assert.Null(attachment);

			documentStore.DatabaseCommands.PutAttachment("ayende", null, new MemoryStream(new byte[] {1, 2, 3}), new RavenJObject {{"Hello", "World"}});

			attachment = documentStore.DatabaseCommands.GetAttachment("ayende");
			Assert.NotNull(attachment);

			Assert.Equal(new byte[] {1, 2, 3}, attachment.Data().ReadData());
			Assert.Equal("World", attachment.Metadata.Value<string>("Hello"));

			documentStore.DatabaseCommands.DeleteAttachment("ayende", null);

			attachment = documentStore.DatabaseCommands.GetAttachment("ayende");
			Assert.Null(attachment);
		}

		[Fact]
		//Fix for issue at http://groups.google.com/group/ravendb/browse_thread/thread/78f1ca6dbdd07e2b
		//The issue only shows up in Server/Client mode, not in Embedded mode!!!
		public void Using_attachments_can_properly_set_WebRequest_Headers()
		{
			var key = string.Format("{0}-{1}", "test", SystemTime.UtcNow.ToFileTimeUtc());
			var metadata = new RavenJObject
			               	{
			               		{"owner", 5},
			               		{"Content-Type", "text/plain"},
			               		{"filename", "test.txt"},
			               		{"Content-Length", 100},
			               	};
			Assert.DoesNotThrow(() => documentStore.DatabaseCommands.PutAttachment(key, null, new MemoryStream(new byte[] {0, 1, 2}), metadata));
		}
	}
}
