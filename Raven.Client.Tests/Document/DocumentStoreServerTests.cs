using System;
using System.IO;
using System.Threading;
using System.Transactions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;
using Raven.Client.Tests.Indexes;
using Raven.Database.Data;
using Raven.Database.Exceptions;
using Raven.Database.Indexing;
using Raven.Server;
using Xunit;
using System.Linq;

namespace Raven.Client.Tests.Document
{
	public class DocumentStoreServerTests : BaseTest, IDisposable
	{
		private readonly string path;
        private readonly int port;

		public DocumentStoreServerTests()
		{
            port = 8080;
            path = GetPath("TestDb");
			RavenDbServer.EnsureCanListenToWhenInNonAdminContext(8080);
		}

		#region IDisposable Members

		public void Dispose()
		{
			Thread.Sleep(100);
			Directory.Delete(path, true);
		}

		#endregion
            
		[Fact]
		public void Should_insert_into_db_and_set_id()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:"+ port };
				documentStore.Initialise();

				var session = documentStore.OpenSession();
				var entity = new Company {Name = "Company"};
				session.Store(entity);
				session.SaveChanges();
				Assert.NotEqual(Guid.Empty.ToString(), entity.Id);
			}
		}

        [Fact]
        public void Can_specify_cutoff_using_server()
        {
            using (var server = GetNewServer(port, path))
            {
                var documentStore = new DocumentStore { Url = "http://localhost:" + port };
                documentStore.Initialise();

                documentStore.DatabaseCommands.Query("Raven/DocumentsByEntityName", new IndexQuery
                {
                    PageSize = 10,
                    Cutoff = DateTime.Now.AddHours(-1)
                });
            }
        }

		[Fact]
		public void Requesting_stats()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialise();

				var session = documentStore.OpenSession();
				var databaseStatistics = session.Load<DatabaseStatistics>("stats");
				Assert.Null(databaseStatistics);
			}
		}


        [Fact]
        public void Can_store_and_get_array_metadata()
        {
            using (var server = GetNewServer(port, path))
            {
                var documentStore = new DocumentStore { Url = "http://localhost:" + port };
                documentStore.Initialise();

                var session = documentStore.OpenSession();
                session.OnEntityConverted += (entity, document, metadata) =>
                {
                    metadata["Raven-Allowed-Users"] = new JArray("ayende", "oren", "rob");
                };

                var company = new Company { Name = "Company" };
                session.Store(company);
                session.SaveChanges();

                var metadataFromServer = session.GetMetadataFor(session.Load<Company>(company.Id));
                var users = metadataFromServer["Raven-Allowed-Users"].OfType<JValue>().Select(x => (string) x.Value).ToArray();
                Assert.Equal(new[]{"ayende","oren","rob"}, users);
            }
        }

		[Fact]
		public void Can_store_using_batch()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialise();
				var batchResults = documentStore
					.DatabaseCommands
					.Batch(new ICommandData[]
					{
						new PutCommandData
						{
							Document = JObject.FromObject(new Company{Name = "Hibernating Rhinos"}),
							Etag = null,
							Key = "rhino1",
							Metadata = new JObject(),
						},
						new PutCommandData
						{
							Document = JObject.FromObject(new Company{Name = "Hibernating Rhinos"}),
							Etag = null,
							Key = "rhino2",
							Metadata = new JObject(),
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
		}


        [Fact]
        public void Can_get_two_documents_in_one_call()
        {
            using (var server = GetNewServer(port, path))
            {
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialise();

                var session = documentStore.OpenSession();
                session.Store(new Company { Name = "Company A", Id = "1"});
                session.Store(new Company { Name = "Company B", Id = "2" });
                session.SaveChanges();


                var session2 = documentStore.OpenSession();

                var companies = session2.Load<Company>("1","2");
                Assert.Equal(2, companies.Length);
                Assert.Equal("Company A", companies[0].Name);
                Assert.Equal("Company B", companies[1].Name);
            }
        }


        [Fact]
        public void Can_delete_document()
        {
            using (var server = GetNewServer(port, path))
            {
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialise();

                var session = documentStore.OpenSession();
                var entity = new Company { Name = "Company" };
                session.Store(entity);
                session.SaveChanges();

                using(var session2 = documentStore.OpenSession())
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
            using (var server = GetNewServer(port, path))
            {
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialise();
                var session = documentStore.OpenSession();
                var company = new Company { Name = "Company 1", Phone = 5 };
                session.Store(company);
                session.SaveChanges();

                documentStore.DatabaseCommands.PutIndex("company_by_name",
                                                        new IndexDefinition
                                                        {
                                                            Map = "from doc in docs where doc.Name != null select new { doc.Name, doc.Phone}",
															Stores = { { "Name", FieldStorage.Yes }, { "Phone", FieldStorage.Yes } }
                                                        });

            	var q = session
                    .LuceneQuery<Company>("company_by_name")
					.SelectFields<Company>("Name","Phone")
            		.WaitForNonStaleResults();
                var single = q.Single();
                Assert.Equal("Company 1", single.Name);
                Assert.Equal(5, single.Phone);
            }
        }

        [Fact]
        public void Can_select_from_index_using_linq_method_chain()
        {
            using (var server = GetNewServer(port, path))
            {
                var documentStore = new DocumentStore { Url = "http://localhost:" + port };
                documentStore.Initialise();
                var session = documentStore.OpenSession();
                var company = new Company { Name = "Company 1", Phone = 5 };
                session.Store(company);
                session.SaveChanges();

                documentStore.DatabaseCommands.PutIndex("company_by_name",
                                                        new IndexDefinition
                                                        {
                                                            Map = "from doc in docs where doc.Name != null select new { doc.Name, doc.Phone}",
                                                            Stores = { { "Name", FieldStorage.Yes }, { "Phone", FieldStorage.Yes } }
                                                        });

                var q = session.Query<Company>("company_by_name")
                    .Customize(query => query.WaitForNonStaleResults())
                    .Where(x => x.Name == company.Name);
                var single = q.ToArray()[0];
                Assert.Equal("Company 1", single.Name);
                Assert.Equal(5, single.Phone);
            }
        }

		[Fact]
		public void Can_select_from_index_using_linq_method_chain_using_integer()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialise();
				var session = documentStore.OpenSession();
				var company = new Company { Name = "Company 1", Phone = 5 };
				session.Store(company);
				session.SaveChanges();

				documentStore.DatabaseCommands.PutIndex("company_by_name",
														new IndexDefinition
														{
															Map = "from doc in docs where doc.Name != null select new { doc.Name, doc.Phone}",
															Stores = { { "Name", FieldStorage.Yes }, { "Phone", FieldStorage.Yes } }
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
        public void Optimistic_concurrency()
        {
            using (var server = GetNewServer(port, path))
            {
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialise();

                var session = documentStore.OpenSession();
                session.UseOptimisticConcurrency = true;
                var company = new Company { Name = "Company 1" };
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
            using (var server = GetNewServer(port, path))
            {
                const string id = "Company/id";
                var documentStore = new DocumentStore { Url = "http://localhost:" + port };
                documentStore.Initialise();

                using (var session = documentStore.OpenSession())
                {
                    Assert.Null(session.Load<Company>(id));
                    using (var tx = new TransactionScope())
                    {
                        var company = new Company { Id = id, Name = "Company 1" };
                        session.Store(company);

                        session.SaveChanges();

                        tx.Complete();
                    }
                    Assert.NotNull(session.Load<Company>(id));

                }
            }
        }

        [Fact]
        public void Can_rollback_transaction_on_insert()
        {
            using (var server = GetNewServer(port, path))
            {
                string id;
                var documentStore = new DocumentStore { Url = "http://localhost:" + port };
                documentStore.Initialise();

                using (var session = documentStore.OpenSession())
                {
                    using (var tx = new TransactionScope())
                    {
                        var company = new Company { Name = "Company 1" };
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
        }

		[Fact]
		public void Should_update_stored_entity()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialise();

				var session = documentStore.OpenSession();
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
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialise();

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
		}

		[Fact]
		public void Should_retrieve_all_entities()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialise();

				var session1 = documentStore.OpenSession();
				session1.Store(new Company {Name = "Company 1"});
				session1.Store(new Company {Name = "Company 2"});

				session1.SaveChanges();

				var session2 = documentStore.OpenSession();
				var companyFound = session2.LuceneQuery<Company>()
					.WaitForNonStaleResults()
					.ToArray();

				Assert.Equal(2, companyFound.Length);
			}
		}

		[Fact]
		public void Can_sort_from_index()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialise();

				var session = documentStore.OpenSession();
				
				session.Store(new Company { Name = "Company 1", Phone = 5 });
				session.Store(new Company { Name = "Company 2", Phone = 3 });
				session.SaveChanges();

				documentStore.DatabaseCommands.PutIndex("company_by_name",
														new IndexDefinition
														{
                                                            Map = "from doc in docs where doc.Name != null select new { doc.Name, doc.Phone}",
															Indexes = { { "Phone", FieldIndexing.Analyzed } }
														});

				// Wait until the index is built
                session.LuceneQuery<Company>("company_by_name")
					.WaitForNonStaleResults()
					.ToArray();

                var companies = session.LuceneQuery<Company>("company_by_name")
					.OrderBy("Phone")
					.WaitForNonStaleResults()
					.ToArray();

				Assert.Equal("Company 2", companies[0].Name);
				Assert.Equal("Company 1", companies[1].Name);
			}
		}

		[Fact]
		public void Can_create_index_using_linq_from_client()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialise();
				documentStore.DatabaseCommands.PutIndex("UsersByLocation", new IndexDefinition<LinqIndexesFromClient.User>
				{
					Map = users => from user in users
								   where user.Location == "Tel Aviv"
								   select new { user.Name },
				});

				using (var session = documentStore.OpenSession())
				{
					session.Store(new LinqIndexesFromClient.User
					{
						Location = "Tel Aviv",
						Name = "Yael"
					});

					session.SaveChanges();

                    LinqIndexesFromClient.User single = session.LuceneQuery<LinqIndexesFromClient.User>("UsersByLocation")
						.Where("Name:Yael")
						.WaitForNonStaleResults()
						.Single();

					Assert.Equal("Yael", single.Name);
				}
			}
		}

		[Fact]
		public void Can_create_index_using_linq_from_client_using_map_reduce()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialise();
				documentStore.DatabaseCommands.PutIndex("UsersCountByLocation", new IndexDefinition<LinqIndexesFromClient.User, LinqIndexesFromClient.LocationCount>
				{
					Map = users => from user in users
								   where user.Location == "Tel Aviv"
								   select new { user.Location, Count =1 },
					Reduce = results => from loc in results 
										group loc by loc.Location into g
										select new { Location = g.Key, Count =  g.Sum(x=>x.Count)},
				});

				using (var session = documentStore.OpenSession())
				{
					session.Store(new LinqIndexesFromClient.User
					{
						Location = "Tel Aviv",
						Name = "Yael"
					});

					session.SaveChanges();

                    LinqIndexesFromClient.LocationCount single = session.LuceneQuery<LinqIndexesFromClient.LocationCount>("UsersCountByLocation")
						.Where("Location:Tel Aviv")
						.WaitForNonStaleResults()
						.Single();

					Assert.Equal("Tel Aviv", single.Location);
					Assert.Equal(1, single.Count);
				}
			}
		}

		[Fact]
		public void Can_get_correct_averages_from_map_reduce_index()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialise();
				documentStore.DatabaseCommands.PutIndex("AvgAgeByLocation", new IndexDefinition<LinqIndexesFromClient.User, LinqIndexesFromClient.LocationAge>
				{
					Map = users => from user in users
								   select new { user.Location, user.Age },
					Reduce = results => from loc in results
										group loc by loc.Location into g
										select new { Location = g.Key, Age = g.Average(x => x.Age) },
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

                    LinqIndexesFromClient.LocationAge single = session.LuceneQuery<LinqIndexesFromClient.LocationAge>("AvgAgeByLocation")
						.Where("Location:Tel Aviv")
						.WaitForNonStaleResults()
						.Single();

					Assert.Equal("Tel Aviv", single.Location);
					Assert.Equal(26.5m, single.Age);
				}
			}
		}
	}
}