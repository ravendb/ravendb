using System;
using System.IO;
using System.Reflection;
using System.Transactions;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;
using Raven.Client.Tests.Indexes;
using Raven.Database.Data;
using Raven.Database.Exceptions;
using Raven.Database.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Client.Tests.Document
{
	public class DocumentStoreEmbeddedTests : BaseTest, IDisposable
	{
		private string path;

		#region IDisposable Members

		public void Dispose()
		{
			Directory.Delete(path, true);
		}

		#endregion

		private DocumentStore NewDocumentStore()
		{
			path = Path.GetDirectoryName(Assembly.GetAssembly(typeof (DocumentStoreServerTests)).CodeBase);
			path = Path.Combine(path, "TestDb").Substring(6);
			var documentStore = new DocumentStore
			{
				Configuration =
					{
						DataDirectory = path,
						RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true
					}
			};
			documentStore.Initialise();
			return documentStore;
		}

        [Fact]
        public void Can_use_transactions_to_isolate_saves()
        {
            using (var documentStore = NewDocumentStore())
            {
                var company = new Company { Name = "Company Name" };
                var session = documentStore.OpenSession();
                using (var tx = new TransactionScope())
                {
                    session.Store(company);
                    session.SaveChanges();

                    using (new TransactionScope(TransactionScopeOption.Suppress))
                    {
                        using (var session2 = documentStore.OpenSession())
                            Assert.Null(session2.Load<Company>(company.Id));

                        tx.Complete();
                    }
                }
                Assert.NotNull(session.Load<Company>(company.Id));
            }  
        }

        [Fact]
        public void Can_refresh_entity_from_database()
        {
            using (var documentStore = NewDocumentStore())
            {
                var company = new Company {Name = "Company Name"};
                var session = documentStore.OpenSession();
                session.Store(company);
                session.SaveChanges();

                var session2 = documentStore.OpenSession();
                var company2 = session2.Load<Company>(company.Id);
                company2.Name = "Hibernating Rhinos";
                session2.Store(company2);
                session2.SaveChanges();

                session.Refresh(company);

                Assert.Equal(company2.Name, company.Name);
              
            }
        }

		[Fact]
		public void Will_set_id_from_query()
		{
			using (var documentStore = NewDocumentStore())
			{
				var company = new Company { Name = "Company Name" };
				using(var session1 = documentStore.OpenSession())
				{
					session1.Store(company);
					session1.SaveChanges();
				}

				using(var session2 = documentStore.OpenSession())
				{
					var companyFromRaven = session2.LuceneQuery<Company>()
						.WaitForNonStaleResults()
						.First();
					Assert.Equal(companyFromRaven.Id, company.Id);
				}
			}
		}

		[Fact]
		public void Will_track_entities_from_query()
		{
			using (var documentStore = NewDocumentStore())
			{
				var company = new Company { Name = "Company Name" };
				using (var session1 = documentStore.OpenSession())
				{
					session1.Store(company);
					session1.SaveChanges();
				}

				using (var session2 = documentStore.OpenSession())
				{
					var companyFromRaven = session2.LuceneQuery<Company>()
						.WaitForNonStaleResults()
						.First();

					companyFromRaven.Name = "Hibernating Rhinos";
					session2.SaveChanges();
				}
				using (var session3 = documentStore.OpenSession())
				{
					var load = session3.Load<Company>(company.Id);
					Assert.Equal("Hibernating Rhinos", load.Name);
				}
			}
		}

        [Fact]
        public void Can_use_transactions_to_isolate_delete()
        {
            using (var documentStore = NewDocumentStore())
            {
                var company = new Company { Name = "Company Name" };
                var session = documentStore.OpenSession();
                session.Store(company);
                session.SaveChanges();
                
                using (var tx = new TransactionScope())
                {
                    session.Delete(company);
                    session.SaveChanges();

                    using (new TransactionScope(TransactionScopeOption.Suppress))
                    {
                        using(var session2 = documentStore.OpenSession())
                            Assert.NotNull(session2.Load<Company>(company.Id));

                    }

					tx.Complete();
                }
                using (var session2 = documentStore.OpenSession())
                    Assert.Null(session2.Load<Company>(company.Id));
            }
        }

		[Fact]
		public void Can_promote_transactions()
		{
			using (var documentStore = NewDocumentStore())
			{
				var company = new Company { Name = "Company Name" };

				using (var tx = new TransactionScope())
				{
					var session = documentStore.OpenSession();
					session.Store(company);
					session.SaveChanges();

					Assert.Equal(Guid.Empty, Transaction.Current.TransactionInformation.DistributedIdentifier);

					using (var session3 = documentStore.OpenSession())
					{
						session3.Store(new Company{ Name = "Another company"});
						session3.SaveChanges();// force a dtc promotion

						Assert.NotEqual(Guid.Empty, Transaction.Current.TransactionInformation.DistributedIdentifier);
					}


					tx.Complete();
				}
				using (var session2 = documentStore.OpenSession())
					Assert.NotNull(session2.Load<Company>(company.Id));
			}
		}


		[Fact]
		public void Will_use_identity_for_document_key()
		{
			using (var documentStore = NewDocumentStore())
			{
				var company = new Company { Name = "Company Name" };
				var session = documentStore.OpenSession();
				session.Store(company);
				session.SaveChanges();

				Assert.Equal("companies/1", company.Id);
			}
		}

        [Fact]
        public void While_in_transaction_can_read_values_private_for_the_Transaction()
        {
            using (var documentStore = NewDocumentStore())
            {
                var company = new Company { Name = "Company Name" };
                var session = documentStore.OpenSession();
                using (new TransactionScope())
                {
                    session.Store(company);
                    session.SaveChanges();

                    Assert.NotNull(session.Load<Company>(company.Id));
                }
            }
        }


        [Fact]
        public void After_tx_rollback_value_will_not_be_in_the_database()
        {
            using (var documentStore = NewDocumentStore())
            {
                var company = new Company { Name = "Company Name" };
                var session = documentStore.OpenSession();
                using (new TransactionScope())
                {
                    session.Store(company);
                    session.SaveChanges();

                }
                using (var session2 = documentStore.OpenSession())
                    Assert.Null(session2.Load<Company>(company.Id));
            }
        }

		[Fact]
		public void Should_Load_entity_back_with_document_Id_mapped_to_Id()
		{
			using (var documentStore = NewDocumentStore())
			{
				var company = new Company {Name = "Company Name"};
				var session = documentStore.OpenSession();
				session.Store(company);

				session.SaveChanges();
				session = documentStore.OpenSession();

				var companyFound = session.Load<Company>(company.Id);

				Assert.Equal(companyFound.Id, company.Id);
			}
		}


		[Fact]
		public void Can_get_entity_back_with_enum()
		{
			using (var documentStore = NewDocumentStore())
			{
				var company = new Company { Name = "Company Name", Type = Company.CompanyType.Private};
				var session = documentStore.OpenSession();
				session.Store(company);

				session.SaveChanges();
				 session = documentStore.OpenSession();
				
				var companyFound = session.Load<Company>(company.Id);

				Assert.Equal(companyFound.Type, company.Type);
			}
		}

		[Fact]
		public void Will_not_store_if_entity_did_not_change()
		{
			var stored = 0;
			using (var documentStore = NewDocumentStore())
			{
				var company = new Company { Name = "Company Name" };
				var session = documentStore.OpenSession();
				session.Stored += o => stored++;
				session.Store(company);
				Assert.Equal(0, stored);
				session.SaveChanges();
				Assert.Equal(1, stored);
				session.SaveChanges();
				Assert.Equal(1, stored);
			}
		}

		[Fact]
		public void Will_store_if_entity_changed()
		{
			var stored = 0;
			using (var documentStore = NewDocumentStore())
			{
				var company = new Company { Name = "Company Name" };
				var session = documentStore.OpenSession();
				session.Store(company);
				Assert.Equal(0, stored);
				session.SaveChanges();

				var sessions2 = documentStore.OpenSession();
				sessions2.Stored += o => stored++;
				var c2 = sessions2.Load<Company>(company.Id);
				sessions2.SaveChanges();
				Assert.Equal(0, stored);
				c2.Phone = 1;
				sessions2.SaveChanges();
				Assert.Equal(1, stored);
			}
		}

		[Fact]
		public void Can_store_using_batch()
		{
			using (var documentStore = NewDocumentStore())
			{
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
		public void Should_map_Entity_Id_to_document_after_save_changes()
		{
			using (var documentStore = NewDocumentStore())
			{
				var session = documentStore.OpenSession();
				var company = new Company {Name = "Company 1"};
				session.Store(company);

				session.SaveChanges();

				Assert.NotEqual(Guid.Empty.ToString(), company.Id);
			}
		}

		[Fact]
		public void Should_update_stored_entity()
		{
			using (var documentStore = NewDocumentStore())
			{
				var session = documentStore.OpenSession();
				var company = new Company {Name = "Company 1"};
				session.Store(company);
				session.SaveChanges();
				var id = company.Id;
				company.Name = "Company 2";
				session.SaveChanges();
                using (var session2 = documentStore.OpenSession())
                {
                    var companyFound = session2.Load<Company>(company.Id);
                    Assert.Equal("Company 2", companyFound.Name);
                    Assert.Equal(id, company.Id);
                }
			}
		}

        [Fact]
        public void Can_project_from_index()
        {
            using (var documentStore = NewDocumentStore())
            {
                var session = documentStore.OpenSession();
                var company = new Company { Name = "Company 1", Phone = 5};
                session.Store(company);
                session.SaveChanges();

                documentStore.DatabaseCommands.PutIndex("company_by_name",
                                                        new IndexDefinition
                                                        {
                                                            Map = "from doc in docs where doc.Name != null select new { doc.Name, doc.Phone}",
															Stores = {{"Name", FieldStorage.Yes}, {"Phone",FieldStorage.Yes}}
                                                        });

            	var q = session
                    .LuceneQuery<Company>("company_by_name")
            		.SelectFields<Company>("Name", "Phone")
            		.WaitForNonStaleResults();
                var single = q.Single();
                Assert.Equal("Company 1", single.Name);
                Assert.Equal(5, single.Phone);
            }
        }

        [Fact]
        public void Can_sort_from_index()
        {
            using (var documentStore = NewDocumentStore())
            {
                var session = documentStore.OpenSession();
                session.Store(new Company { Name = "Company 1", Phone = 5 });
                session.Store(new Company { Name = "Company 2", Phone = 3 });
                session.SaveChanges();

            	documentStore.DatabaseCommands.PutIndex("company_by_name",
            	                                        new IndexDefinition
            	                                        {
                                                            Map = "from doc in docs where doc.Name != null select new { doc.Name, doc.Phone}",
            	                                        	Indexes = {{"Phone", FieldIndexing.Analyzed}}
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
        public void Optimistic_concurrency()
        {
            using (var documentStore = NewDocumentStore())
            {
                var session = documentStore.OpenSession();
                session.UseOptimisticConcurrency = true;
                var company = new Company { Name = "Company 1" };
                session.Store(company);
                session.SaveChanges();

                using(var session2 = documentStore.OpenSession())
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
		public void Should_update_retrieved_entity()
		{
			using (var documentStore = NewDocumentStore())
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
		}

        [Fact]
        public void Session_implements_unit_of_work()
        {
            using (var documentStore = NewDocumentStore())
            {
                var session1 = documentStore.OpenSession();
                var company = new Company { Name = "Company 1" };
                session1.Store(company);
                session1.SaveChanges();

                Assert.Same(company, session1.Load<Company>(company.Id));

                var companyId = company.Id;

                var session2 = documentStore.OpenSession();
                Assert.Same(session2.Load<Company>(companyId), session2.Load<Company>(companyId));
            }
        }

		[Fact]
		public void Should_retrieve_all_entities()
		{
			using (var documentStore = NewDocumentStore())
			{
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
		public void Can_create_index_using_linq_from_client()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("UsersByLocation", new IndexDefinition<LinqIndexesFromClient.User>
				{
					Map = users => from user in users
								   where user.Location == "Tel Aviv"
								   select new { user.Name },
				});

				using (var session = store.OpenSession())
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
	}
}