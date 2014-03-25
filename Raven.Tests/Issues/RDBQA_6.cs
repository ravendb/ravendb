// -----------------------------------------------------------------------
//  <copyright file="RDBQA_6.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Smuggler;
using Raven.Client.Extensions;
using Raven.Database.Extensions;
using Raven.Smuggler;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RDBQA_6 : RavenTest
    {
	    [Fact]
        public async Task SmugglerShouldThrowIfDatabaseDoesNotExist()
        {
	        var backupPath = Path.GetTempFileName();
		    using (var store = NewRemoteDocumentStore())
		    {
			    var smuggler = new SmugglerApi();

				var e = await AssertAsync.Throws<SmugglerException>(async () => await smuggler.ImportData(new SmugglerImportOptions { FromFile = backupPath, To = new RavenConnectionStringOptions { Url = store.Url, DefaultDatabase = "DoesNotExist" } }, new SmugglerOptions()));
			    Assert.Equal("Smuggler does not support database creation (database 'DoesNotExist' on server 'http://localhost:8079' must exist before running Smuggler).", e.Message);

			    e = await AssertAsync.Throws<SmugglerException>(async () =>
			    {
				    var exportData = await smuggler.ExportData(new SmugglerExportOptions(), new SmugglerOptions());
				    IOExtensions.DeleteDirectory(exportData.FilePath);
			    });
			    Assert.Equal("Smuggler does not support database creation (database 'DoesNotExist' on server 'http://localhost:8079' must exist before running Smuggler).", e.Message);
		    }
        }

	    [Fact]
	    public async Task SmugglerShouldNotThrowIfDatabaseExist1()
	    {
		    var backupPath = Path.GetTempFileName();

		    using (var store = NewRemoteDocumentStore())
		    {
			    store.DatabaseCommands.ForSystemDatabase().EnsureDatabaseExists("DoesNotExist");

			    var smuggler = new SmugglerApi();

				await smuggler.ImportData(new SmugglerImportOptions { FromFile = backupPath, To = new RavenConnectionStringOptions { Url = store.Url, DefaultDatabase = "DoesNotExist" } }, new SmugglerOptions());
				await smuggler.ExportData(new SmugglerExportOptions { From = new RavenConnectionStringOptions { Url = store.Url, DefaultDatabase = "DoesNotExist" }, ToFile = backupPath }, new SmugglerOptions());
		    }
	    }

	    [Fact]
        public async Task SmugglerShouldNotThrowIfDatabaseExist2()
        {
			var backupPath = Path.GetTempFileName();

		    using (var store = NewRemoteDocumentStore())
		    {
			    var smuggler = new SmugglerApi();

				await smuggler.ImportData(new SmugglerImportOptions { FromFile = backupPath, To = new RavenConnectionStringOptions { Url = store.Url } }, new SmugglerOptions());
				await smuggler.ExportData(new SmugglerExportOptions { From = new RavenConnectionStringOptions { Url = store.Url } , ToFile = backupPath}, new SmugglerOptions());
		    }
        }

        [Fact]
        public async Task SmugglerBehaviorWhenServerIsDown()
        {
			var backupPath = Path.GetTempFileName();
	        var smuggler = new SmugglerApi();

			var e = await AssertAsync.Throws<SmugglerException>(() => smuggler.ImportData(new SmugglerImportOptions { FromFile = backupPath, To = new RavenConnectionStringOptions { Url = "http://localhost:8078/", DefaultDatabase = "DoesNotExist" } }, new SmugglerOptions()));

	        Assert.Equal("Smuggler encountered a connection problem: 'Unable to connect to the remote server'.", e.Message);

			e = await AssertAsync.Throws<SmugglerException>(() => smuggler.ExportData(new SmugglerExportOptions { From = new RavenConnectionStringOptions { Url = "http://localhost:8078/", DefaultDatabase = "DoesNotExist" } , ToFile = backupPath}, new SmugglerOptions()));

	        Assert.Equal("Smuggler encountered a connection problem: 'Unable to connect to the remote server'.", e.Message);
        }
    }
}