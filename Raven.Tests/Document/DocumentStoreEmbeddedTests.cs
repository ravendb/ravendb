//-----------------------------------------------------------------------
// <copyright file="DocumentStoreEmbeddedTests.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Transactions;
using Raven.Json.Linq;
using Raven.Client.Client;
using Raven.Client.Exceptions;
using Raven.Client.Indexes;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Database.Json;
using Raven.Http.Exceptions;
using Raven.Tests.Indexes;
using Xunit;
using System.Linq;

namespace Raven.Tests.Document
{
	public class DocumentStoreEmbeddedTests : RemoteClientTest, IDisposable
	{
		private string path;

		#region IDisposable Members

		public void Dispose()
		{
            IOExtensions.DeleteDirectory(path);
		}

		#endregion

        private EmbeddableDocumentStore NewDocumentStore()
		{
			path = Path.GetDirectoryName(Assembly.GetAssembly(typeof (DocumentStoreServerTests)).CodeBase);
			path = Path.Combine(path, "TestDb").Substring(6);
            var documentStore = new EmbeddableDocumentStore
			{
				Configuration =
					{
						DataDirectory = path,
						RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true
					}
			};
			documentStore.Initialize();
	
			new RavenDocumentsByEntityName().Execute(documentStore);
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
		public void Can_get_index_names()
		{
			using (var documentStore = NewDocumentStore())
			{
				Assert.Contains("Raven/DocumentsByEntityName",
					documentStore.DatabaseCommands.GetIndexNames(0, 25));
			}
		}

		[Fact]
		public void Can_reset_builtin_index()
		{
			using (var documentStore = NewDocumentStore())
			{
				documentStore.DocumentDatabase.ResetIndex("Raven/DocumentsByEntityName");
			}
		}

		[Fact]
		public void Using_attachments()
		{
			using (var documentStore = NewDocumentStore())
			{
				var attachment = documentStore.DatabaseCommands.GetAttachment("ayende");
				Assert.Null(attachment);

				documentStore.DatabaseCommands.PutAttachment("ayende", null, new byte[] {1, 2, 3}, new RavenJObject {{"Hello", "World"}});

				attachment = documentStore.DatabaseCommands.GetAttachment("ayende");
				Assert.NotNull(attachment);

				Assert.Equal(new byte[]{1,2,3}, attachment.Data);
				Assert.Equal("World", attachment.Metadata.Value<string>("Hello"));

				documentStore.DatabaseCommands.DeleteAttachment("ayende", null);

				attachment = documentStore.DatabaseCommands.GetAttachment("ayende");
				Assert.Null(attachment);

			}
		}


		[Fact]
		public void Will_get_notification_when_reading_non_authoritive_information()
		{
			using (var documentStore = NewDocumentStore())
			{
				var company = new Company { Name = "Company Name" };
				var session = documentStore.OpenSession();
				using (var original = documentStore.OpenSession())
				{
					original.Store(company);
					original.SaveChanges();
				}
				using ( new TransactionScope())
				{
					company.Name = "Another Name";
					session.Store(company);
					session.SaveChanges();

					using (new TransactionScope(TransactionScopeOption.Suppress))
					{
						using (var session2 = documentStore.OpenSession())
						{
                            session2.Advanced.AllowNonAuthoritiveInformation = false;
                            session2.Advanced.NonAuthoritiveInformationTimeout = TimeSpan.Zero;
							Assert.Throws<NonAuthoritiveInformationException>(()=>session2.Load<Company>(company.Id));
						}
					}
				}
			}
		}

		[Fact]
		public void Will_process_all_different_documents_enlisted_in_a_transaction()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var tx = new TransactionScope())
				{
					using (var session = documentStore.OpenSession())
					{
						// Remark: Don't change the order of the stored classes!
						// This test will only fail if the classes are not
						// stored in their alphabetical order!
						session.Store(new Contact {FirstName = "Contact"});
						session.Store(new Company {Name = "Company"});
						session.SaveChanges();
					}
					tx.Complete();
				}
				Thread.Sleep(500);
				using (var session = documentStore.OpenSession())
				{
					Assert.NotNull(session.Load<Contact>("contacts/1"));
					Assert.NotNull(session.Load<Company>("companies/1"));
					session.SaveChanges();
				}
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

                session.Advanced.Refresh(company);

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
                    var companyFromRaven = session2.Advanced.LuceneQuery<Company>()
						.WaitForNonStaleResults()
						.First();
					Assert.Equal(companyFromRaven.Id, company.Id);
				}
			}
		}


		[Fact]
		public void Can_escape_special_characters()
		{
			using (var documentStore = NewDocumentStore())
			{
				documentStore.DatabaseCommands.PutIndex("Companies/Name", new IndexDefinition<Company>
				{
					Map = companies => from c in companies
					                   select new {c.Name}
				});
				var company = new Company { Name = "http://hibernatingrhinos.com" };
				using (var session1 = documentStore.OpenSession())
				{
					session1.Store(company);
					session1.SaveChanges();
				}

				using (var session2 = documentStore.OpenSession())
				{
					var companyFromRaven = session2.Query<Company>("Companies/Name")
						.Customize(query => query.WaitForNonStaleResults())
						.Where(x=>x.Name.Contains(company.Name))
						.ToArray()
						.First();
					Assert.Equal(companyFromRaven.Id, company.Id);
				}
			}
		}

		[Fact]
		public void Can_get_index_def()
		{
			using (var documentStore = NewDocumentStore())
			{
				documentStore.DatabaseCommands.PutIndex("Companies/Name", new IndexDefinition<Company, Company>
				{
					Map = companies => from c in companies
					                   select new {c.Name},
					Indexes = {{x => x.Name, FieldIndexing.NotAnalyzed}}
				});
				var indexDefinition = documentStore.DatabaseCommands.GetIndex("Companies/Name");
				Assert.Equal(@"docs.Companies
	.Select(c => new {Name = c.Name})", indexDefinition.Map);
				Assert.Equal(FieldIndexing.NotAnalyzed, indexDefinition.Indexes["Name"]);
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
                    var companyFromRaven = session2.Advanced.LuceneQuery<Company>()
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
            	for (int i = 0; i < 15; i++)// wait for commit
            	{
					using (var session2 = documentStore.OpenSession())
						if (session2.Load<Company>(company.Id) == null)
							break;
					Thread.Sleep(100);
            	}
				using (var session2 = documentStore.OpenSession())
					Assert.Null(session2.Load<Company>(company.Id));
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
                session.Advanced.Stored += o => stored++;
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
                sessions2.Advanced.Stored += o => stored++;
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
							Document = RavenJObject.FromObject(new Company{Name = "Hibernating Rhinos"}),
							Etag = null,
							Key = "rhino1",
							Metadata = new RavenJObject(),
						},
						new PutCommandData
						{
							Document = RavenJObject.FromObject(new Company{Name = "Hibernating Rhinos"}),
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
															Stores = {{"Name", FieldStorage.Yes}, {"Phone",FieldStorage.Yes}},
                                                            Indexes = { { "Name", FieldIndexing.NotAnalyzed}},
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
        public void Optimistic_concurrency()
        {
            using (var documentStore = NewDocumentStore())
            {
                var session = documentStore.OpenSession();
                session.Advanced.UseOptimisticConcurrency = true;
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
                var companyFound = session2.Advanced.LuceneQuery<Company>()
					.WaitForNonStaleResults()
					.ToArray();

				Assert.Equal(2, companyFound.Length);
			}
		}

		[Fact]
		public void Should_retrieve_all_entities_using_connection_string()
		{
            using (var documentStore = new EmbeddableDocumentStore
			{
				ConnectionStringName = "Local",
				Configuration =
					{
						RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true
					}
			})
			{
				path = documentStore.DataDirectory;

				documentStore.Initialize();

				var session1 = documentStore.OpenSession();
				session1.Store(new Company { Name = "Company 1" });
				session1.Store(new Company { Name = "Company 2" });

				session1.SaveChanges();
				var session2 = documentStore.OpenSession();
                var companyFound = session2.Advanced.LuceneQuery<Company>()
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

                    LinqIndexesFromClient.User single = session.Advanced.LuceneQuery<LinqIndexesFromClient.User>("UsersByLocation")
						.Where("Name:Yael")
						.WaitForNonStaleResults()
						.Single();

					Assert.Equal("Yael", single.Name);
				}
			}
		}

		[Fact]
		public void Can_delete_by_index()
		{
			using (var store = NewDocumentStore())
			{
				var entity = new Company { Name = "Company" };
				using (var session = store.OpenSession())
				{
					session.Store(entity);
					session.SaveChanges();

                    session.Advanced.LuceneQuery<Company>().WaitForNonStaleResults().ToArray();// wait for the index to settle down
				}

				store.DatabaseCommands.DeleteByIndex("Raven/DocumentsByEntityName", new IndexQuery
				{
					Query = "Tag:[[Companies]]"
				}, allowStale: false);

				using (var session = store.OpenSession())
				{
                    Assert.Empty(session.Advanced.LuceneQuery<Company>().WaitForNonStaleResults().ToArray());
				}
			}
		}

		[Fact]
		public void Can_update_by_index()
		{
			using (var store = NewDocumentStore())
			{
				var entity = new Company { Name = "Company" };
				using (var session = store.OpenSession())
				{
					session.Store(entity);
					session.SaveChanges();

                    session.Advanced.LuceneQuery<Company>().WaitForNonStaleResults().ToArray();// wait for the index to settle down
				}

				store.DatabaseCommands.UpdateByIndex("Raven/DocumentsByEntityName", new IndexQuery
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

				using (var session = store.OpenSession())
				{
					Assert.Equal("Another Company", session.Load<Company>(entity.Id).Name);
				}
			}
		}

	}
}
