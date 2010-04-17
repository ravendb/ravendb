using System;
using System.IO;
using System.Reflection;
using System.Transactions;
using Raven.Client.Document;
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
				DataDirectory = path,
				Conventions = {FindIdentityProperty = q => q.Name == "Id"}
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

                        tx.Complete();
                    }
                }
                using (var session2 = documentStore.OpenSession())
                    Assert.Null(session2.Load<Company>(company.Id));
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

				var companyFound = session.Load<Company>(company.Id);

				Assert.Equal(companyFound.Id, company.Id);
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
                                                            Map = "from doc in docs select new { doc.Name, doc.Phone}"
                                                        });

                var q = from doc in session
                            .Query<Company>("company_by_name")
                            .WaitForNonStaleResults()
                        select new {doc.Name, doc.Phone};
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
            	                                        	Map = "from doc in docs select new { doc.Name, doc.Phone}",
            	                                        	Indexes = {{"Phone", FieldIndexing.Tokenized}}
            	                                        });

				// Wait until the index is built
				session.Query<Company>("company_by_name")
					.WaitForNonStaleResults()
					.ToArray();

                var companies = session.Query<Company>("company_by_name")
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
				var companyFound = session2.Query<Company>()
					.WaitForNonStaleResults()
					.ToArray();

				Assert.Equal(2, companyFound.Length);
			}
		}
	}
}