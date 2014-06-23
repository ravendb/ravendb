using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Security.Policy;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Database.Config;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2380 : RavenTestBase
	{
		private class Foo
		{
			public string Name { get; set; }
		}

		protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
		{
			configuration.Settings[Constants.BulkImportTimeout] = 500.ToString(CultureInfo.InvariantCulture);
			configuration.BulkImportTimeoutInMs = 500;
		}

		[Fact]
		public void Server_side_timeout_should_close_connection()
		{
			using (var store = NewRemoteDocumentStore(databaseName: "TestDB"))
			{
				using (var bulkInsertOp = store.BulkInsert("TestDB", new BulkInsertOptions { BatchSize = 1 }))
				{
					bulkInsertOp.Store(new Foo { Name = "bar1" }, "foo/bar/1");
					bulkInsertOp.Store(new Foo { Name = "bar2" }, "foo/bar/2");

					Thread.Sleep(501);

					Assert.Throws<Exception>(() => bulkInsertOp.Store(new Foo { Name = "bar3" }, "foo/bar/3"));
				}
			}
		}

		[Fact]
		public void Server_down_should_abort_bulk_insert_operation()
		{
			var server = GetNewServer();
			using (var store = new DocumentStore
			{
				Url = server.Configuration.ServerUrl,
				DefaultDatabase = "TestDB"
			})
			{
				store.Initialize();
				using (var bulkInsertOp = store.BulkInsert("TestDB", new BulkInsertOptions { BatchSize = 1 }))
				{
					bulkInsertOp.Store(new Foo { Name = "bar1" }, "foo/bar/1");
					bulkInsertOp.Store(new Foo { Name = "bar2" }, "foo/bar/2");

					server.Dispose();

					Assert.Throws<Exception>(() => bulkInsertOp.Store(new Foo { Name = "bar3" }, "foo/bar/3"));
				}
			}
		}
	}
}
