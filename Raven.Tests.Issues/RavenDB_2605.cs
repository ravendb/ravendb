// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2605.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;

using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Client.Extensions;
using Raven.Database.Smuggler;
using Raven.Smuggler;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2605 : RavenTest
	{
		[Fact]
		public void MaxNumberOfItemsToProcessInSingleBatchShouldBeRespectedByDataDumper()
		{
			var path = Path.Combine(NewDataPath(forceCreateDir: true), "raven.dump");

			using (var server = GetNewServer(configureConfig: configuration => configuration.MaxNumberOfItemsToProcessInSingleBatch = 1234))
			{
                var dumper = new DatabaseDataDumper(server.SystemDatabase, options: new SmugglerDatabaseOptions { BatchSize = 4321 });
				Assert.Equal(4321, dumper.Options.BatchSize);

                dumper.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { ToFile = path }).ResultUnwrap();

				Assert.Equal(1234, dumper.Options.BatchSize);

                dumper = new DatabaseDataDumper(server.SystemDatabase, options: new SmugglerDatabaseOptions { BatchSize = 4321 });
				Assert.Equal(4321, dumper.Options.BatchSize);

                dumper.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = path }).Wait();

				Assert.Equal(1234, dumper.Options.BatchSize);

                dumper = new DatabaseDataDumper(server.SystemDatabase, options: new SmugglerDatabaseOptions { BatchSize = 1000 });
				Assert.Equal(1000, dumper.Options.BatchSize);

                dumper.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { ToFile = path }).ResultUnwrap();

				Assert.Equal(1000, dumper.Options.BatchSize);
			}
		}

		[Fact]
		public void MaxNumberOfItemsToProcessInSingleBatchShouldBeRespectedBySmuggler()
		{
			var path = Path.Combine(NewDataPath(forceCreateDir: true), "raven.dump");

			using (var server = GetNewServer(configureConfig: configuration => configuration.MaxNumberOfItemsToProcessInSingleBatch = 1234))
			{
                var smuggler = new SmugglerDatabaseApi(options: new SmugglerDatabaseOptions { BatchSize = 4321 });
				Assert.Equal(4321, smuggler.Options.BatchSize);

                smuggler.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { ToFile = path, From = new RavenConnectionStringOptions { Url = server.Configuration.ServerUrl } }).ResultUnwrap();

				Assert.Equal(1234, smuggler.Options.BatchSize);

                smuggler = new SmugglerDatabaseApi(options: new SmugglerDatabaseOptions { BatchSize = 4321 });
				Assert.Equal(4321, smuggler.Options.BatchSize);

                smuggler.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = path, To = new RavenConnectionStringOptions { Url = server.Configuration.ServerUrl } }).Wait();

				Assert.Equal(1234, smuggler.Options.BatchSize);

                smuggler = new SmugglerDatabaseApi(options: new SmugglerDatabaseOptions { BatchSize = 1000 });
				Assert.Equal(1000, smuggler.Options.BatchSize);

                smuggler.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { ToFile = path, From = new RavenConnectionStringOptions { Url = server.Configuration.ServerUrl } }).ResultUnwrap();

				Assert.Equal(1000, smuggler.Options.BatchSize);
			}
		}
	}
}