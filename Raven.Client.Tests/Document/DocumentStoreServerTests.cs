using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Transactions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Tests.Indexes;
using Raven.Database.Data;
using Raven.Database.Exceptions;
using Raven.Database.Indexing;
using Raven.Database.Json;
using Raven.Database.Server;
using Raven.Server;
using Xunit;
using System.Linq;

using Raven.Tests.Spatial;

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
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8080);
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
				documentStore.Initialize();

				var session = documentStore.OpenSession();
				var entity = new Company {Name = "Company"};
				session.Store(entity);
				session.SaveChanges();
				Assert.NotEqual(Guid.Empty.ToString(), entity.Id);
			}
		}

		[Fact]
		public void Can_get_index_def()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialize();

				documentStore.DatabaseCommands.PutIndex("Companies/Name", new IndexDefinition<Company, Company>
				{
					Map = companies => from c in companies
									   select new { c.Name },
					Indexes = { { x => x.Name, FieldIndexing.NotAnalyzed } }
				});
				var indexDefinition = documentStore.DatabaseCommands.GetIndex("Companies/Name");
				Assert.Equal(@"docs.Companies
	.Select(c => new {Name = c.Name})", indexDefinition.Map);
				Assert.Equal(FieldIndexing.NotAnalyzed, indexDefinition.Indexes["Name"]);
			}
		}

		[Fact]
		public void Can_delete_by_index()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialize();

				var entity = new Company { Name = "Company" };
				using (var session = documentStore.OpenSession())
				{
					session.Store(entity);
					session.SaveChanges();

					session.LuceneQuery<Company>().WaitForNonStaleResults().ToArray();// wait for the index to settle down
				}

				documentStore.DatabaseCommands.DeleteByIndex("Raven/DocumentsByEntityName", new IndexQuery
				{
					Query = "Tag:[[Companies]]"
				}, allowStale: false);

				using (var session = documentStore.OpenSession())
				{
					Assert.Empty(session.LuceneQuery<Company>().WaitForNonStaleResults().ToArray());
				}
			}
		}


		[Fact]
		public void Can_order_by_using_linq()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialize();

				documentStore.DatabaseCommands.PutIndex("CompaniesByName", new IndexDefinition
				{
					Map = "from company in docs.Companies select new { company.Name, company.Phone }",
				});

				using (var session = documentStore.OpenSession())
				{
					session.Store(new Company { Name = "A", Phone = 4 });
					session.Store(new Company { Name = "B", Phone = 3 });
					session.Store(new Company { Name = "B", Phone = 4 });
					session.SaveChanges();

					session.LuceneQuery<Company>("CompaniesByName").WaitForNonStaleResults().ToArray();// wait for the index to settle down
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
						orderby company.Phone, company.Name
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
		}

		
       [Fact]
       public void Can_create_index_with_decimal_as_firstfield()
       {
           using (var server = GetNewServer(port, path))
           {
               var documentStore = new DocumentStore { Url = "http://localhost:" + port };
               documentStore.Initialize();
               var session = documentStore.OpenSession();
               var company = new Company { Name = "Company 1", Phone= 5, AccountsReceivable = (decimal)3904.39 };
               session.Store(company);
               session.SaveChanges();

			   documentStore.DatabaseCommands.PutIndex("company_by_name",
                                                       new IndexDefinition
                                                       {
                                                           Map = "from doc in docs where doc.Name != null select new { doc.AccountsReceivable, doc.Name}",
                                                           Stores = { { "Name", FieldStorage.Yes }, { "AccountsReceivable", FieldStorage.Yes } }
                                                       });

               var q = session.Query<Company>("company_by_name")
                   .Customize(query => query.WaitForNonStaleResults(TimeSpan.FromHours(1)));
               var single = q.ToList().SingleOrDefault();

               Assert.NotNull(single);
               Assert.Equal("Company 1", single.Name);
               Assert.Equal((decimal)3904.39, single.AccountsReceivable);
           }
       }

		[Fact] 
        public void  Can_select_from_index_using_linq_method_chain_using_decimal_and_greater_than_or_equal() 
        { 
            using (var server = GetNewServer(port, path)) 
            { 
                var documentStore = new DocumentStore { Url = "http://localhost:" + port }; 
                documentStore.Initialize(); 
                var session = documentStore.OpenSession(); 
                var company = new Company { Name = "Company 1", Phone = 5, AccountsReceivable = (decimal)3904.39 }; 
                session.Store(company); 
                session.SaveChanges(); 
				documentStore.DatabaseCommands.PutIndex("company_by_name", new 
														IndexDefinition 
                                                        { 
                                                            Map = "from doc in docs where doc.Name != null select new { doc.Name, doc.AccountsReceivable}", 
                                                            Stores = { { "Name", FieldStorage.Yes }, { "AccountsReceivable", FieldStorage.Yes } } 
                                                        }); 
                var q = session.Query<Company>("company_by_name") 
                    .Customize(query => query.WaitForNonStaleResults(TimeSpan.FromHours(1))) 
                    .Where(x => x.AccountsReceivable > 1); 
                var single = q.ToList().SingleOrDefault(); 
                Assert.NotNull(single); 
                Assert.Equal("Company 1", single.Name); 
                Assert.Equal((decimal)3904.39, single.AccountsReceivable); 
            } 
        } 

       [Fact]
       public void Can_create_index_with_decimal_as_lastfield()
       {
           using (var server = GetNewServer(port, path))
           {
               var documentStore = new DocumentStore { Url = "http://localhost:" + port };
               documentStore.Initialize();
               var session = documentStore.OpenSession();
               var company = new Company { Name = "Company 1", Phone= 5, AccountsReceivable = (decimal)3904.39 };
               session.Store(company);
               session.SaveChanges();
			   
			   documentStore.DatabaseCommands.PutIndex("company_by_name",new IndexDefinition
                                                       {
                                                           Map = "from doc in docs where doc.Name != null select new {  doc.Name, doc.AccountsReceivable }",
                                                           Stores = { { "Name", FieldStorage.Yes }, { "AccountsReceivable", FieldStorage.Yes } }
                                                       });

               var q = session.Query<Company>("company_by_name")
                   .Customize(query => query.WaitForNonStaleResults(TimeSpan.FromHours(1)));
               var single = q.ToList().SingleOrDefault();

               Assert.NotNull(single);
               Assert.Equal("Company 1", single.Name);
               Assert.Equal((decimal)3904.39, single.AccountsReceivable);
           }
       }

		[Fact]
		public void Can_update_by_index()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialize();

				var entity = new Company { Name = "Company" };
				using (var session = documentStore.OpenSession())
				{
					session.Store(entity);
					session.SaveChanges();

					session.LuceneQuery<Company>().WaitForNonStaleResults().ToArray();// wait for the index to settle down
				}

				documentStore.DatabaseCommands.UpdateByIndex("Raven/DocumentsByEntityName", new IndexQuery
				{
					Query = "Tag:[[Companies]]"
				}, new[]
				{
					new PatchRequest
					{
						Type = "Set",
						Name = "Name",
						Value = JToken.FromObject("Another Company")
					},
				}, allowStale: false);

				using (var session = documentStore.OpenSession())
				{
					Assert.Equal("Another Company", session.Load<Company>(entity.Id).Name);
				}
			}
		}


        [Fact]
        public void Can_specify_cutoff_using_server()
        {
            using (var server = GetNewServer(port, path))
            {
                var documentStore = new DocumentStore { Url = "http://localhost:" + port };
                documentStore.Initialize();

                documentStore.DatabaseCommands.Query("Raven/DocumentsByEntityName", new IndexQuery
                {
                    PageSize = 10,
                    Cutoff = DateTime.Now.AddHours(-1)
                }, null);
            }
        }

		[Fact]
		public void Can_read_projected_dates()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialize();


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
					var query = s.LuceneQuery<object>("my_index")
						.SelectFields<DateHolder>("Date")
						.WaitForNonStaleResults();
					var dateHolder = query.ToArray().First();

					Assert.Equal(new DateTime(2000, 1, 1), dateHolder.Date);
				}
			}
		}

		public class DateHolder
		{
			public DateTime Date { get; set; }
		}

        [Fact]
		public void Can_query_using_special_characters()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialize();

				
				documentStore.DatabaseCommands.PutIndex("my_index",
													new IndexDefinition
													{
														Map = "from doc in docs select new { doc.Language, doc.Type}",
														Stores = { { "Name", FieldStorage.Yes }, { "Phone", FieldStorage.Yes } }
													});

				using(var s = documentStore.OpenSession())
				{
					s.Store(new
					{
						Language = "Français",//Note the ç
						Type = "Feats"
					});
					s.SaveChanges();
				}

				using (var s = documentStore.OpenSession())
				{
					var query = s.LuceneQuery<object>("my_index")
						.Where("Type:Feats AND Language:Français")
						.WaitForNonStaleResults();
					query.ToArray();

					Assert.Equal(1, query.QueryResult.TotalResults);
				}
			}
		}

		[Fact]
		public void Can_query_indexes_returning_complex_objects()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialize();


				documentStore.DatabaseCommands.PutIndex("my_index",
													new IndexDefinition
													{
														Map = "from doc in docs select new { doc.Language, doc.Type, Value = new{ Answers = 42, Paths = 7 }  }}",
														Stores = { { "Value", FieldStorage.Yes },  }
													});

				using (var s = documentStore.OpenSession())
				{
					s.Store(new
					{
						Language = "Français",//Note the ç
						Type = "Feats"
					});
					s.SaveChanges();
				}

				using (var s = documentStore.OpenSession())
				{
					var query = s.LuceneQuery<object>("my_index")
						.Where("Type:Feats AND Language:Français")
						.SelectFields<object>("Value")
						.WaitForNonStaleResults();
					var first = (JObject)query.First();

					Assert.Equal(42, first.Value<JObject>("Value").Value<int>("Answers"));
					Assert.Equal(7, first.Value<JObject>("Value").Value<int>("Paths"));
				}
			}
		}

		[Fact]
		public void Requesting_stats()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialize();

				var session = documentStore.OpenSession();
				var databaseStatistics = session.Load<DatabaseStatistics>("stats");
				Assert.Null(databaseStatistics);
			}
		}



		[Fact]
		public void Can_get_entity_back_with_enum()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialize();

				var company = new Company { Name = "Company Name", Type = Company.CompanyType.Private };
				var session = documentStore.OpenSession();
				session.Store(company);

				session.SaveChanges();
				session = documentStore.OpenSession();
				
				var companyFound = session.Load<Company>(company.Id);

				Assert.Equal(companyFound.Type, company.Type);
			}
		}

        [Fact]
        public void Can_store_and_get_array_metadata()
        {
            using (var server = GetNewServer(port, path))
            {
                var documentStore = new DocumentStore { Url = "http://localhost:" + port };
                documentStore.Initialize();

                var session = documentStore.OpenSession();
                session.OnEntityConverted += (entity, document, metadata) =>
                {
					metadata["Raven-Allowed-Users"] = new JArray("ayende", "oren", "rob");
                };

                var company = new Company { Name = "Company" };
                session.Store(company);
                session.SaveChanges();

                var metadataFromServer = session.GetMetadataFor(session.Load<Company>(company.Id));
				var users = metadataFromServer["Raven-Allowed-Users"].OfType<JValue>().Select(x => (string)x.Value).ToArray();
                Assert.Equal(new[]{"ayende","oren","rob"}, users);
            }
        }

		[Fact]
		public void Can_store_using_batch()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialize();
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
				documentStore.Initialize();

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
				documentStore.Initialize();

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
				documentStore.Initialize();
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
                documentStore.Initialize();
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
                    .Where(x => x.Name.Contains(company.Name));
                var single = q.ToArray()[0];
                Assert.Equal("Company 1", single.Name);
                Assert.Equal(5, single.Phone);
            }
        }

		[Fact]
		public void Can_select_from_index_using_linq_method_chain_with_variable()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialize();
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

				var name = company.Name;
				var q = session.Query<Company>("company_by_name")
					.Customize(query => query.WaitForNonStaleResults())
					.Where(x => x.Name.Contains(name));
				var single = q.ToArray()[0];
				Assert.Equal("Company 1", single.Name);
				Assert.Equal(5, single.Phone);
			}
		}

		[Fact]
		public void Can_select_from_index_using_linq_with_no_where_clause()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialize();
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
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialize();
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
		public void Can_select_from_index_using_linq_method_chain_using_integer_and_greater_than_or_equal()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialize();
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
            using (var server = GetNewServer(port, path))
            {
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialize();

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
                documentStore.Initialize();

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
                documentStore.Initialize();

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
				documentStore.Initialize();

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
				documentStore.Initialize();

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
				documentStore.Initialize();

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
		public void Should_retrieve_all_entities_using_connection_string()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { ConnectionStringName = "Server"};
				documentStore.Initialize();

				var session1 = documentStore.OpenSession();
				session1.Store(new Company { Name = "Company 1" });
				session1.Store(new Company { Name = "Company 2" });

				session1.SaveChanges();

				var session2 = documentStore.OpenSession();
				var companyFound = session2.LuceneQuery<Company>()
					.WaitForNonStaleResults()
					.ToArray();

				Assert.Equal(2, companyFound.Length);
			}
		}

		[Fact]
		public void Can_read_credentials_from_connection_string()
		{
			var documentStore = new DocumentStore {ConnectionStringName = "Secure"};
			Assert.NotNull(documentStore.Credentials);
			var networkCredential = (NetworkCredential)documentStore.Credentials;
			Assert.Equal("beam", networkCredential.UserName);
			Assert.Equal("up", networkCredential.Password);
		}

		[Fact]
		public void Can_sort_from_index()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialize();

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
		public void Can_query_from_spatial_index()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialize();

				var session = documentStore.OpenSession();

				foreach (Event @event in SpatialIndexTestHelper.GetEvents())
				{
					session.Store(@event);
				}

				session.SaveChanges();

				var indexDefinition = new IndexDefinition
				{
					Map = "from e in docs.Events select new { Tag = \"Event\" }",
					Indexes = {
						{ "Tag", FieldIndexing.NotAnalyzed }
					}
				}
				.ToSpatial("e.Latitude", "e.Longitude");

				documentStore.DatabaseCommands.PutIndex("eventsByLatLng", indexDefinition);

				// Wait until the index is built
				session.LuceneQuery<Event>("eventsByLatLng")
					.WaitForNonStaleResults()
					.ToArray();

				const double lat = 38.96939, lng = -77.386398;
				const double miles = 6.0;

				var events = session.LuceneQuery<Event>("eventsByLatLng")
					.WhereEquals("Tag", "Event")
					.WithinRadiusOfLatLng(miles, lat, lng)
					.WaitForNonStaleResults()
					.ToArray();

				Assert.Equal(7, events.Length);

				foreach (var e in events)
				{
					double distance = Raven.Database.Indexing.SpatialIndex.GetDistanceMi(lat, lng, e.Latitude, e.Longitude);
					Console.WriteLine("Venue: " + e.Venue + ", Distance " + distance);
					Assert.True(distance < miles);
				}
			}
		}

		[Fact]
		public void Can_create_index_using_linq_from_client()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialize();
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
				documentStore.Initialize();
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
				documentStore.Initialize();
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

        [Fact]
        public void Can_get_correct_maximum_from_map_reduce_index() {
            using (var server = GetNewServer(port, path))
			{
                var documentStore = new DocumentStore { Url = "http://localhost:" + port };
                documentStore.Initialize();

                documentStore.DatabaseCommands.PutIndex("MaxAge", new IndexDefinition<LinqIndexesFromClient.User, LinqIndexesFromClient.LocationAge> {
                    Map = users => from user in users
                                   select new { user.Age },
					Indexes = {{x=>x.Age, FieldIndexing.Analyzed}},
					Stores = {{x=>x.Age, FieldStorage.Yes}}
                });

                using (var session = documentStore.OpenSession()) {

                    session.Store(new LinqIndexesFromClient.User {
                        Age = 27,
                        Name = "Foo"
                    });

                    session.Store(new LinqIndexesFromClient.User {
                        Age = 33,
                        Name = "Bar"
                    });

                    session.Store(new LinqIndexesFromClient.User {
                        Age = 29,
                        Name = "Bar"
                    });

                    session.SaveChanges();

					var user = session.LuceneQuery<LinqIndexesFromClient.User>("MaxAge")
						.OrderBy("-Age")
						.Take(1)
                        .WaitForNonStaleResults()
                        .Single();

                    Assert.Equal(33, user.Age);
                }
            }
        }

		[Fact]
		public void Using_attachments()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialize();

				var attachment = documentStore.DatabaseCommands.GetAttachment("ayende");
				Assert.Null(attachment);

				documentStore.DatabaseCommands.PutAttachment("ayende", null, new byte[] { 1, 2, 3 }, new JObject(new JProperty("Hello", "World")));

				attachment = documentStore.DatabaseCommands.GetAttachment("ayende");
				Assert.NotNull(attachment);

				Assert.Equal(new byte[] { 1, 2, 3 }, attachment.Data);
				Assert.Equal("World", attachment.Metadata.Value<string>("Hello"));

				documentStore.DatabaseCommands.DeleteAttachment("ayende", null);

				attachment = documentStore.DatabaseCommands.GetAttachment("ayende");
				Assert.Null(attachment);

			}
		}
	}
}