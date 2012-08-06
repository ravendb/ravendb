//-----------------------------------------------------------------------
// <copyright file="DocumentStoreEmbeddedTests.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using System.Threading;
using System.Transactions;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Embedded;
using Raven.Json.Linq;
using Raven.Client.Exceptions;
using Raven.Client.Indexes;
using Raven.Database.Extensions;
using Raven.Tests.Indexes;
using Xunit;
using System.Linq;

namespace Raven.Tests.Document
{
	public class DocumentStoreEmbeddedTests : RemoteClientTest, IDisposable
	{
		private readonly EmbeddableDocumentStore documentStore;

		public DocumentStoreEmbeddedTests()
		{
			documentStore = NewDocumentStore();
		}

		public override void Dispose()
		{
			documentStore.Dispose();
			base.Dispose();
		}

		[Fact]
		public void CanUseTransactionsToIsolateSaves()
		{
			var company = new Company { Name = "Company Name" };
			using (var session = documentStore.OpenSession())
			{
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
		public void CanGetIndexNames()
		{
			Assert.Contains("Raven/DocumentsByEntityName", documentStore.DatabaseCommands.GetIndexNames(0, 25));
		}

		[Fact]
		public void CanResetBuiltinIndex()
		{
			documentStore.DocumentDatabase.ResetIndex("Raven/DocumentsByEntityName");
		}

		[Fact]
		public void UsingAttachments()
		{
			var attachment = documentStore.DatabaseCommands.GetAttachment("ayende");
			Assert.Null(attachment);

			documentStore.DatabaseCommands.PutAttachment("ayende", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject { { "Hello", "World" } });

			attachment = documentStore.DatabaseCommands.GetAttachment("ayende");
			Assert.NotNull(attachment);

			Assert.Equal(new byte[] { 1, 2, 3 }, attachment.Data().ReadData());
			Assert.Equal("World", attachment.Metadata.Value<string>("Hello"));

			documentStore.DatabaseCommands.DeleteAttachment("ayende", null);

			attachment = documentStore.DatabaseCommands.GetAttachment("ayende");
			Assert.Null(attachment);
		}


		[Fact]
		public void WillGetNotificationWhenReadingNonAuthoritativeInformation()
		{
			var company = new Company { Name = "Company Name" };
			using (var session = documentStore.OpenSession())
			{
				using (var original = documentStore.OpenSession())
				{
					original.Store(company);
					original.SaveChanges();
				}
				using (new TransactionScope())
				{
					session.Load<Company>(company.Id).Name = "Another Name";
					session.SaveChanges();

					using (new TransactionScope(TransactionScopeOption.Suppress))
					{
						using (var session2 = documentStore.OpenSession())
						{
							session2.Advanced.AllowNonAuthoritativeInformation = false;
							session2.Advanced.NonAuthoritativeInformationTimeout = TimeSpan.Zero;
							Assert.Throws<NonAuthoritativeInformationException>(() => session2.Load<Company>(company.Id));
						}
					}
				}
			}
		}

		[Fact]
		public void WillProcessAllDifferentDocumentsEnlistedInATransaction()
		{
			using (var tx = new TransactionScope())
			{
				using (var session = documentStore.OpenSession())
				{
					// Remark: Don't change the order of the stored classes!
					// This test will only fail if the classes are not
					// stored in their alphabetical order!
					session.Store(new Contact { FirstName = "Contact" });
					session.Store(new Company { Name = "Company" });
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

		[Fact]
		public void CanRefreshEntityFromDatabase()
		{
			var company = new Company { Name = "Company Name" };
			using (var session = documentStore.OpenSession())
			{
				session.Store(company);
				session.SaveChanges();

				using (var session2 = documentStore.OpenSession())
				{
					var company2 = session2.Load<Company>(company.Id);
					company2.Name = "Hibernating Rhinos";
					session2.Store(company2);
					session2.SaveChanges();
				}

				session.Advanced.Refresh(company);
				Assert.Equal("Hibernating Rhinos", company.Name);
			}
		}

		[Fact]
		public void WillSetIdFromQuery()
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
				Assert.Equal(companyFromRaven.Id, company.Id);
			}
		}


		[Fact]
		public void CanEscapeSpecialCharacters()
		{
			documentStore.DatabaseCommands.PutIndex("Companies/Name", new IndexDefinitionBuilder<Company>
																		{
																			Map = companies => from c in companies
																							   select new { c.Name }
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
					.Where(x => x.Name == (company.Name))
					.ToArray()
					.First();
				Assert.Equal(companyFromRaven.Id, company.Id);
			}
		}

		[Fact]
		public void CanGetIndexDef()
		{
			documentStore.DatabaseCommands.PutIndex("Companies/Name", new IndexDefinitionBuilder<Company, Company>
																		{
																			Map = companies => from c in companies
																							   select new { c.Name },
																			Indexes = { { x => x.Name, FieldIndexing.NotAnalyzed } }
																		});
			var indexDefinition = documentStore.DatabaseCommands.GetIndex("Companies/Name");
			Assert.Equal(@"docs.Companies.Select(c => new {
    Name = c.Name
})", indexDefinition.Map);
			Assert.Equal(FieldIndexing.NotAnalyzed, indexDefinition.Indexes["Name"]);
		}


		[Fact]
		public void WillTrackEntitiesFromQuery()
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

		[Fact]
		public void CanUseTransactionsToIsolateDelete()
		{
			var company = new Company { Name = "Company Name" };
			using (var session = documentStore.OpenSession())
			{
				session.Store(company);
				session.SaveChanges();

				using (var tx = new TransactionScope())
				{
					session.Delete(company);
					session.SaveChanges();

					using (new TransactionScope(TransactionScopeOption.Suppress))
					{
						using (var session2 = documentStore.OpenSession())
							Assert.NotNull(session2.Load<Company>(company.Id));
					}

					tx.Complete();
				}
				for (int i = 0; i < 15; i++) // wait for commit
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
		public void WillUseIdentityForDocumentKey()
		{
			var company = new Company { Name = "Company Name" };
			using (var session = documentStore.OpenSession())
			{
				session.Store(company);
				session.SaveChanges();

				Assert.Equal("companies/1", company.Id);
			}
		}

		[Fact]
		public void WhileInTransactionCanReadValuesPrivateForTheTransaction()
		{
			var company = new Company { Name = "Company Name" };
			using (var session = documentStore.OpenSession())
			{
				using (new TransactionScope())
				{
					session.Store(company);
					session.SaveChanges();

					Assert.NotNull(session.Load<Company>(company.Id));
				}
			}
		}


		[Fact]
		public void AfterTxRollbackValueWillNotBeInTheDatabase()
		{
			var company = new Company { Name = "Company Name" };
			using (var session = documentStore.OpenSession())
			{
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
		public void ShouldLoadEntityBackWithDocumentIdMappedToId()
		{
			var company = new Company { Name = "Company Name" };
			using (var session = documentStore.OpenSession())
			{
				session.Store(company);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				var companyFound = session.Load<Company>(company.Id);
				Assert.Equal(companyFound.Id, company.Id);
			}
		}

		[Fact]
		public void CanGetEntityBackWithEnum()
		{
			var company = new Company { Name = "Company Name", Type = Company.CompanyType.Private };
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
		public void WillNotStoreIfEntityDidNotChange()
		{
			var company = new Company { Name = "Company Name" };
			using (var session = documentStore.OpenSession())
			{
				session.Store(company);
				session.SaveChanges();
				session.SaveChanges();

				Assert.Equal(1, session.Advanced.NumberOfRequests);
			}
		}

		[Fact]
		public void WillStoreIfEntityChanged()
		{
			var company = new Company { Name = "Company Name" };
			using (var session = documentStore.OpenSession())
			{
				session.Store(company);
				session.SaveChanges();
			}

			using (var session2 = documentStore.OpenSession())
			{
				var c2 = session2.Load<Company>(company.Id);
				session2.SaveChanges();
				c2.Phone = 1;
				session2.SaveChanges();
				Assert.Equal(2, session2.Advanced.NumberOfRequests);
			}
		}

		[Fact]
		public void CanStoreUsingBatch()
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
		public void CanGetDocumentMetadata()
		{
			documentStore.DatabaseCommands
				.Put("rhino1", null, RavenJObject.FromObject(new Company { Name = "Hibernating Rhinos" }), new RavenJObject());

			JsonDocument doc = documentStore.DatabaseCommands.Get("rhino1");
			JsonDocumentMetadata meta = documentStore.DatabaseCommands.Head("rhino1");

			Assert.NotNull(meta);
			Assert.Equal(doc.Key, meta.Key);
			Assert.Equal(doc.Etag, meta.Etag);
			Assert.Equal(doc.LastModified, meta.LastModified);
		}

		[Fact]
		public void WhenDocumentDoesNotExistThenMetadataShouldBeNull()
		{
			Assert.Null(documentStore.DatabaseCommands.Head("rhino1"));
		}

		[Fact]
		public void ShouldMapEntityIdToDocumentAfterSaveChanges()
		{
			using (var session = documentStore.OpenSession())
			{
				var company = new Company { Name = "Company 1" };
				session.Store(company);

				session.SaveChanges();

				Assert.NotEqual(Guid.Empty.ToString(), company.Id);
			}
		}

		[Fact]
		public void ShouldUpdateStoredEntity()
		{
			using (var session = documentStore.OpenSession())
			{
				var company = new Company { Name = "Company 1" };
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
		public void CanProjectFromIndex()
		{
			using (var session = documentStore.OpenSession())
			{
				var company = new Company { Name = "Company 1", Phone = 5 };
				session.Store(company);
				session.SaveChanges();

				documentStore.DatabaseCommands.PutIndex("company_by_name",
														new IndexDefinition
														{
															Map = "from doc in docs where doc.Name != null select new { doc.Name, doc.Phone}",
															Stores = { { "Name", FieldStorage.Yes }, { "Phone", FieldStorage.Yes } },
															Indexes = { { "Name", FieldIndexing.NotAnalyzed } },
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
		public void CanSortFromIndex()
		{
			using (var session = documentStore.OpenSession())
			{
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
		public void OptimisticConcurrency()
		{
			using (var session = documentStore.OpenSession())
			{
				session.Advanced.UseOptimisticConcurrency = true;
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
		public void ShouldUpdateRetrievedEntity()
		{
			var company = new Company { Name = "Company 1" };
			using (var session1 = documentStore.OpenSession())
			{
				session1.Store(company);
				session1.SaveChanges();
			}

			var companyId = company.Id;

			using (var session2 = documentStore.OpenSession())
			{
				var companyFound = session2.Load<Company>(companyId);
				companyFound.Name = "New Name";
				session2.SaveChanges();

				Assert.Equal("New Name", session2.Load<Company>(companyId).Name);
			}
		}

		[Fact]
		public void SessionImplementsUnitOfWork()
		{
			var company = new Company { Name = "Company 1" };
			using (var session1 = documentStore.OpenSession())
			{
				session1.Store(company);
				session1.SaveChanges();

				Assert.Same(company, session1.Load<Company>(company.Id));
			}

			var companyId = company.Id;
			using (var session2 = documentStore.OpenSession())
			{
				Assert.Same(session2.Load<Company>(companyId), session2.Load<Company>(companyId));
			}
		}

		[Fact]
		public void ShouldRetrieveAllEntities()
		{
			using (var session1 = documentStore.OpenSession())
			{
				session1.Store(new Company { Name = "Company 1" });
				session1.Store(new Company { Name = "Company 2" });

				session1.SaveChanges();
			}

			using (var session2 = documentStore.OpenSession())
			{
				var companyFound = session2.Advanced.LuceneQuery<Company>()
					.WaitForNonStaleResults()
					.ToArray();

				Assert.Equal(2, companyFound.Length);
			}
		}

		[Fact]
		public void CanCreateIndexUsingLinqFromClient()
		{
			documentStore.DatabaseCommands.PutIndex("UsersByLocation", new IndexDefinitionBuilder<LinqIndexesFromClient.User>
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

				LinqIndexesFromClient.User single = session.Advanced.LuceneQuery<LinqIndexesFromClient.User>("UsersByLocation")
					.Where("Name:Yael")
					.WaitForNonStaleResults()
					.Single();

				Assert.Equal("Yael", single.Name);
			}
		}

		[Fact]
		public void CanDeleteByIndex()
		{
			var entity = new Company { Name = "Company" };
			using (var session = documentStore.OpenSession())
			{
				session.Store(entity);
				session.SaveChanges();

				session.Advanced.LuceneQuery<Company>().WaitForNonStaleResults().ToArray();// wait for the index to settle down
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
		public void CanUpdateByIndex()
		{
			var entity = new Company { Name = "Company" };
			using (var session = documentStore.OpenSession())
			{
				session.Store(entity);
				session.SaveChanges();

				session.Advanced.LuceneQuery<Company>().WaitForNonStaleResults().ToArray();// wait for the index to settle down
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
	}
}