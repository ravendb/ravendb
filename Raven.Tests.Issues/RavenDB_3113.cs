using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3113 : RavenTestBase
	{
		
		[Fact]
		public void Should_work_with_embeddable()
		{
			using (var store = new EmbeddableDocumentStore())
			{				
				store.Configuration.RunInMemory = true;
				store.Configuration.Settings[Constants.ActiveBundles] = "Versioning";
				store.Configuration.DefaultStorageTypeName = "voron";
				store.Initialize();

				DoTest(store);
			}
		}

		[Fact]
		public void Should_work_with_remote()
		{
			using (var server = GetNewServer())
			using (var store = new DocumentStore())
			{
				store.Url = server.Configuration.ServerUrl;
				store.Initialize();
				store.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
				{
					Id = "Raven/Databases/FooDB",
					Settings =
					{
						{ Constants.ActiveBundles, "Versioning"},
						{ Constants.RavenDataDir, "~/Data"}
					}
				});

				DoTest(store,"FooDB");
			}
		}


		private static void DoTest(IDocumentStore store, string database = null)
		{
			int id;

			if (database == null)
			{
				using (var session = store.OpenSession())
				{
					session.Store(new
					{
						Exclude = false,
						Id = "Raven/Versioning/DefaultConfiguration",
						MaxRevisions = int.MaxValue
					});

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var testDocument = new TestDocument{ Id = 1 };
					session.Store(testDocument);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var document = session.Load<TestDocument>(1);
					var metadata = session.Advanced.GetMetadataFor(document);

					Assert.True(metadata.ContainsKey("Raven-Document-Revision"));
				}
			}
			else
			{
				using (var session = store.OpenSession(database))
				{
					session.Store(new
					{
						Exclude = false,
						Id = "Raven/Versioning/DefaultConfiguration",
						MaxRevisions = int.MaxValue
					});

					session.SaveChanges();
				}

				using (var session = store.OpenSession(database))
				{
					var testDocument = new TestDocument { Id = 1 };
					session.Store(testDocument);
					session.SaveChanges();
				}

				using (var session = store.OpenSession(database))
				{
					var document = session.Load<TestDocument>(1);
					var metadata = session.Advanced.GetMetadataFor(document);

					Assert.True(metadata.ContainsKey("Raven-Document-Revision"));
				}
				
			}
		}

		public class TestDocument
		{
			public int Id { get; set; }

			public string TestProperty { get; set; }
		}
	}
}
