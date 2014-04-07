// -----------------------------------------------------------------------
//  <copyright file="RDBQA_6.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;

using Raven.Database.Extensions;
using Raven.Tests.Common;

namespace Raven.Tests.Issues
{
    using System.Threading.Tasks;

    using Raven.Abstractions.Data;
    using Raven.Abstractions.Exceptions;
    using Raven.Abstractions.Smuggler;
    using Raven.Client.Extensions;
    using Raven.Smuggler;

    using Xunit;

    public class RDBQA_6 : RavenTest
    {
        [Fact]
        public async Task SmugglerShouldThrowIfDatabaseDoesNotExist()
        {
            var path = Path.GetTempFileName();

            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    var smuggler = new SmugglerApi();

                    var e = await AssertAsync.Throws<SmugglerException>(() => smuggler.ImportData(new SmugglerImportOptions
                    {
                        FromFile = path,
                        To = new RavenConnectionStringOptions
                        {
                            Url = store.Url,
                            DefaultDatabase = "DoesNotExist"
                        }
                    }, new SmugglerOptions()));

                    Assert.Equal(string.Format("Smuggler does not support database creation (database 'DoesNotExist' on server '{0}' must exist before running Smuggler).", store.Url), e.Message);

                    e = await AssertAsync.Throws<SmugglerException>(() => smuggler.ExportData(new SmugglerExportOptions
                    {
                        ToFile = path,
                        From = new RavenConnectionStringOptions
                        {
                            Url = store.Url,
                            DefaultDatabase = "DoesNotExist"
                        }
                    }, new SmugglerOptions()));

                    Assert.Equal(string.Format("Smuggler does not support database creation (database 'DoesNotExist' on server '{0}' must exist before running Smuggler).", store.Url), e.Message);
                }
            }
            finally
            {
                IOExtensions.DeleteFile(path);
            }
        }

        [Fact]
        public async Task SmugglerShouldNotThrowIfDatabaseExist1()
        {
            var path = Path.GetTempFileName();

            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    store.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists("DoesNotExist");

                    var smuggler = new SmugglerApi();

                    await smuggler.ImportData(new SmugglerImportOptions
                    {
                        FromFile = path,
                        To = new RavenConnectionStringOptions
                        {
                            Url = store.Url,
                            DefaultDatabase = "DoesNotExist"
                        }
                    }, new SmugglerOptions());
                    await smuggler.ExportData(new SmugglerExportOptions
                    {
                        ToFile = path,
                        From = new RavenConnectionStringOptions
                        {
                            Url = store.Url,
                            DefaultDatabase = "DoesNotExist"
                        }
                    }, new SmugglerOptions());
                }
            }
            finally
            {
                IOExtensions.DeleteFile(path);
            }
        }

        [Fact]
        public async Task SmugglerShouldNotThrowIfDatabaseExist2()
        {
            var path = Path.GetTempFileName();

            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    var smuggler = new SmugglerApi();

                    await smuggler.ImportData(new SmugglerImportOptions
                    {
                        FromFile = path,
                        To = new RavenConnectionStringOptions
                        {
                            Url = store.Url,
                            DefaultDatabase = store.DefaultDatabase
                        }
                    }, new SmugglerOptions());
                    await smuggler.ExportData(new SmugglerExportOptions
                    {
                        ToFile = path,
                        From = new RavenConnectionStringOptions
                        {
                            Url = store.Url,
                            DefaultDatabase = store.DefaultDatabase
                        }
                    }, new SmugglerOptions());
                }
            }
            finally
            {
                IOExtensions.DeleteFile(path);
            }
        }

        [Fact]
        public async Task SmugglerBehaviorWhenServerIsDown()
        {
            var path = Path.GetTempFileName();

            try
            {
                var smuggler = new SmugglerApi();

                var e = await AssertAsync.Throws<SmugglerException>(() => smuggler.ImportData(new SmugglerImportOptions
                {
                    FromFile = path,
                    To = new RavenConnectionStringOptions
                    {
                        Url = "http://localhost:8078/",
                        DefaultDatabase = "DoesNotExist"
                    }
                }, new SmugglerOptions()));

                Assert.Contains("Smuggler encountered a connection problem:", e.Message);

                e = await AssertAsync.Throws<SmugglerException>(() => smuggler.ExportData(new SmugglerExportOptions
                {
                    ToFile = path,
                    From = new RavenConnectionStringOptions
                    {
                        Url = "http://localhost:8078/",
                        DefaultDatabase = "DoesNotExist"
                    }
                }, new SmugglerOptions()));

                Assert.Contains("Smuggler encountered a connection problem:", e.Message);
            }
            finally
            {
                IOExtensions.DeleteFile(path);
            }
        }
    }
}