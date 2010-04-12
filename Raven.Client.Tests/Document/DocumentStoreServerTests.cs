using System;
using System.IO;
using System.Reflection;
using System.Threading;
using Raven.Client.Document;
using Raven.Database;
using Raven.Server;
using Xunit;
using System.Linq;

namespace Raven.Client.Tests
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
				var documentStore = new DocumentStore("localhost", port);
				documentStore.Initialise();

				var session = documentStore.OpenSession();
				var entity = new Company {Name = "Company"};
				session.Store(entity);

				Assert.NotEqual(Guid.Empty.ToString(), entity.Id);
			}
		}

		[Fact]
		public void Should_update_stored_entity()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore("localhost", port);
				documentStore.Initialise();

				var session = documentStore.OpenSession();
				var company = new Company {Name = "Company 1"};
				session.Store(company);
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
				var documentStore = new DocumentStore("localhost", port);
				documentStore.Initialise();

				var session1 = documentStore.OpenSession();
				var company = new Company {Name = "Company 1"};
				session1.Store(company);
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
				var documentStore = new DocumentStore("localhost", port);
				documentStore.Initialise();

				var session1 = documentStore.OpenSession();
				session1.Store(new Company {Name = "Company 1"});
				session1.Store(new Company {Name = "Company 2"});

				var session2 = documentStore.OpenSession();
				var companyFound = session2.Query<Company>().ToArray();

				Assert.Equal(2, companyFound.Length);
			}
		}
	}
}