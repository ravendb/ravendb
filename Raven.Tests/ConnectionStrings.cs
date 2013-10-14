using Raven.Client.Document;
using Xunit;

namespace Raven.Tests
{
	public class ConnectionStrings : RavenTest
	{

		[Fact]
		public void check_url()
		{
			using (var store = new DocumentStore())
			{
				store.ParseConnectionString("Url=http://localhost:8079/;");

				Assert.Equal("http://localhost:8079", store.Url);
				Assert.Equal("http://localhost:8079", store.Identifier);
				Assert.NotNull(store.ResourceManagerId);
				Assert.NotNull(store.Credentials);
				Assert.Null(store.DefaultDatabase);
				Assert.True(store.EnlistInDistributedTransactions);
			}
		}

		[Fact]
		public void check_illegal_connstrings()
		{
			using (var store = new DocumentStore())
			{
				Assert.Throws<System.ArgumentException>(() => store.ParseConnectionString(string.Empty));
			}
		}

		[Fact]
		public void check_url_and_rmid()
		{
			using (var store = new DocumentStore())
			{
				store.ParseConnectionString("Url=http://localhost:8079/;ResourceManagerId=d5723e19-92ad-4531-adad-8611e6e05c8a;");

				Assert.Equal("http://localhost:8079", store.Url);
				Assert.Equal("http://localhost:8079", store.Identifier);
				Assert.NotNull(store.Credentials);
				Assert.Null(store.DefaultDatabase);
				Assert.True(store.EnlistInDistributedTransactions);
			}
		}

		[Fact]
		public void check_defaultdb()
		{
			using (var store = new DocumentStore())
			{
				store.ParseConnectionString("DefaultDatabase=DevMachine;");

				Assert.Null(store.Url);
				Assert.Null(store.Identifier);
				Assert.NotNull(store.ResourceManagerId);
				Assert.NotNull(store.Credentials);
				Assert.Equal("DevMachine", store.DefaultDatabase);
				Assert.True(store.EnlistInDistributedTransactions);
			}
		}

		[Fact]
		public void check_url_and_defaultdb()
		{
			using (var store = new DocumentStore())
			{
				store.ParseConnectionString("Url=http://localhost:8079/;DefaultDatabase=DevMachine;");

				Assert.Equal("http://localhost:8079", store.Url);
				Assert.Equal("http://localhost:8079 (DB: DevMachine)", store.Identifier);
				Assert.NotNull(store.ResourceManagerId);
				Assert.NotNull(store.Credentials);
				Assert.Equal("DevMachine", store.DefaultDatabase);
				Assert.True(store.EnlistInDistributedTransactions);
			}
		}

		[Fact]
		public void can_work_with_default_db()
		{
			using (var store = new DocumentStore())
			{
				store.ParseConnectionString("Url=http://localhost:8079/;DefaultDatabase=DevMachine;ResourceManagerId=d5723e19-92ad-4531-adad-8611e6e05c8a;");

				Assert.Equal("http://localhost:8079", store.Url);
				Assert.Equal("http://localhost:8079 (DB: DevMachine)", store.Identifier);
				Assert.NotNull(store.Credentials);
				Assert.Equal("DevMachine", store.DefaultDatabase);
				Assert.True(store.EnlistInDistributedTransactions);
			}
		}

		[Fact]
		public void Can_get_api_key()
		{
			using (var store = new DocumentStore())
			{
				store.ParseConnectionString("Url=http://localhost:8079/;ApiKey=d5723e19-92ad-4531-adad-8611e6e05c8a;");

				Assert.Equal("d5723e19-92ad-4531-adad-8611e6e05c8a", store.ApiKey);
			}
		}

		[Fact]
		public void Can_get_failover_urls()
		{
			using (var store = new DocumentStore())
			{
				store.ParseConnectionString("Url=http://localhost:8079/;FailoverUrl=http://localhost:8078/;FailoverUrl=http://localhost:8077/databases/test;FailoverUrl=Northwind|http://localhost:8076/");

				Assert.Equal("http://localhost:8079", store.Url);
				Assert.Equal(2, store.FailoverServers.ForDefaultDatabase.Length);
				Assert.Equal("http://localhost:8078", store.FailoverServers.ForDefaultDatabase[0]);
				Assert.Equal("http://localhost:8077/databases/test", store.FailoverServers.ForDefaultDatabase[1]);
				Assert.Equal(1, store.FailoverServers.GetForDatabase("Northwind").Length);
				Assert.Equal("http://localhost:8076", store.FailoverServers.GetForDatabase("Northwind")[0]);
			}
		}
	}
}
