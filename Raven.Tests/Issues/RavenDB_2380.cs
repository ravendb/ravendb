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
	
		[Fact]
		public void FlushingTimeout_should_abort_BulkInsert()
		{
			using(var store = NewRemoteDocumentStore(databaseName:"TestDB"))
			{
				using (var bulkInsertOp = store.BulkInsert("TestDB", new BulkInsertOptions { WriteTimeoutMilliseconds = 2*1000}))
				{
					bulkInsertOp.Store(new Foo { Name = "bar1" }, "foo/bar/1");
					bulkInsertOp.Store(new Foo { Name = "bar2" }, "foo/bar/2");

					Thread.Sleep(TimeSpan.FromSeconds(3));

					Assert.Throws<InvalidOperationException>(() => bulkInsertOp.Store(new Foo { Name = "bar3" }, "foo/bar/3"));
				}
			}
		}

		protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
		{
		    configuration.Settings[Constants.BulkImportBatchTimeout] = TimeSpan.FromMilliseconds(250).ToString();
		}
	}
}
