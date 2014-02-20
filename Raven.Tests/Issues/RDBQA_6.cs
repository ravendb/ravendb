// -----------------------------------------------------------------------
//  <copyright file="RDBQA_6.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Issues
{
    using System;
    using System.IO;
    using System.Net;
    using System.Threading.Tasks;

    using Raven.Abstractions.Data;
    using Raven.Abstractions.Exceptions;
    using Raven.Abstractions.Smuggler;
    using Raven.Client.Extensions;
    using Raven.Database.Extensions;
    using Raven.Smuggler;

    using Xunit;

    public class RDBQA_6 : RavenTest
    {
        [Fact]
        public async Task SmugglerShouldThrowIfDatabaseDoesNotExist()
        {
            var options = new SmugglerOptions
            {
                BackupPath = Path.GetTempFileName()
            };

            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    var smuggler = new SmugglerApi(options, new RavenConnectionStringOptions { Url = store.Url, DefaultDatabase = "DoesNotExist" });

                    var e = await AssertAsync.Throws<SmugglerException>(() => smuggler.ImportData(options));

                    Assert.Equal("Smuggler does not support database creation (database 'DoesNotExist' on server 'http://localhost:8079' must exist before running Smuggler).", e.Message);

                    e = await AssertAsync.Throws<SmugglerException>(() => smuggler.ExportData(null, options, false));

                    Assert.Equal("Smuggler does not support database creation (database 'DoesNotExist' on server 'http://localhost:8079' must exist before running Smuggler).", e.Message);
                }
            }
            finally
            {
                IOExtensions.DeleteDirectory(options.BackupPath);
            }
        }

        [Fact]
        public async Task SmugglerShouldNotThrowIfDatabaseExist1()
        {
            var options = new SmugglerOptions
            {
                BackupPath = Path.GetTempFileName()
            };

            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    store.DatabaseCommands.ForSystemDatabase().EnsureDatabaseExists("DoesNotExist");

                    var smuggler = new SmugglerApi(options, new RavenConnectionStringOptions { Url = store.Url, DefaultDatabase = "DoesNotExist" });

                    await smuggler.ImportData(options);
                    await smuggler.ExportData(null, options, false);
                }
            }
            finally
            {
                IOExtensions.DeleteDirectory(options.BackupPath);
            }
        }

        [Fact]
        public async Task SmugglerShouldNotThrowIfDatabaseExist2()
        {
            var options = new SmugglerOptions
            {
                BackupPath = Path.GetTempFileName()
            };

            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    var smuggler = new SmugglerApi(options, new RavenConnectionStringOptions { Url = store.Url });

                    await smuggler.ImportData(options);
                    await smuggler.ExportData(null, options, false);
                }
            }
            finally
            {
                IOExtensions.DeleteDirectory(options.BackupPath);
            }
        }

        [Fact]
        public async Task SmugglerBehaviorWhenServerIsDown()
        {
            var options = new SmugglerOptions
            {
                BackupPath = Path.GetTempFileName()
            };

            try
            {
                    var smuggler = new SmugglerApi(options, new RavenConnectionStringOptions { Url = "http://localhost:8078/", DefaultDatabase = "DoesNotExist" });

                    var e = await AssertAsync.Throws<SmugglerException>(() => smuggler.ImportData(options));

                    Assert.Equal("Smuggler encountered a connection problem: 'Unable to connect to the remote server'.", e.Message);

                    e = await AssertAsync.Throws<SmugglerException>(() => smuggler.ExportData(null, options, false));

                    Assert.Equal("Smuggler encountered a connection problem: 'Unable to connect to the remote server'.", e.Message);
            }
            finally
            {
                IOExtensions.DeleteDirectory(options.BackupPath);
            }
        }
    }
}