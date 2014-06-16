using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database.Config;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Document
{
	public class BulkImportTimeout : RavenTestBase
	{
		private InMemoryRavenConfiguration _configuration;

		protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
		{
			configuration.Settings[Constants.BulkImportTimeout] = 500.ToString(CultureInfo.InvariantCulture);
			_configuration = configuration;
		}

		[Fact]
		public void During_bulk_import_no_new_documents_more_than_threshold_disconnect_connection()
		{
			using (var store = NewRemoteDocumentStore(fiddler: true))
			{
				using (var bulkImportOperation = store.BulkInsert(options: new BulkInsertOptions {BatchSize = 1}))
				{
					bulkImportOperation.Store(new {Foo = "Bar"}, "Foo/Bar1");

				    Thread.Sleep(_configuration.BulkImportTimeoutInMs*5);

					bulkImportOperation.Store(new { Foo = "Bar" }, "Foo/Bar2");
				}
			}
		}
	}
}
