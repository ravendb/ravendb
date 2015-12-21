using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Smuggler;
using Raven.Client.Extensions;
using Raven.Json.Linq;
using Raven.Smuggler;
using Raven.Tests.Common;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3736_2 : RavenTestBase
    {
        [Fact]
        public async Task SmugglingResivionDocsIntoDatabaseWithVersioningEnabled()
        {
            using (var server = GetNewServer())
            {
                var path = Path.GetTempFileName();
                using (var store = NewRemoteDocumentStore(ravenDbServer: server))
                {
                    store
                        .DatabaseCommands
                        .GlobalAdmin
                        .CreateDatabase(new DatabaseDocument
                        {
                            Id = "Source",
                            Settings =
                            {
                                {Constants.ActiveBundles, "Versioning"},
                                {"Raven/DataDir", NewDataPath()}
                            }
                        });
                    store.DatabaseCommands.EnsureDatabaseExists("Source");

                    store
                        .DatabaseCommands
                        .GlobalAdmin
                        .CreateDatabase(new DatabaseDocument
                        {
                            Id = "Dest",
                            Settings =
                            {
                                {Constants.ActiveBundles, "Versioning"},
                                {"Raven/DataDir", NewDataPath()}
                            }
                        });
                    store.DatabaseCommands.EnsureDatabaseExists("Dest");


                    store.DatabaseCommands.ForDatabase("Source")
                        .Put("Raven/Versioning/DefaultConfiguration", null, RavenJObject.FromObject(new Raven.Bundles.Versioning.Data.VersioningConfiguration
                        {
                            Exclude = false,
                            Id = "DefaultConfiguration",
                            MaxRevisions = 5
                        }), new RavenJObject());
                    store.DatabaseCommands.ForDatabase("Dest")
                        .Put("Raven/Versioning/DefaultConfiguration", null, RavenJObject.FromObject(new Raven.Bundles.Versioning.Data.VersioningConfiguration
                        {
                            Exclude = false,
                            Id = "DefaultConfiguration",
                            MaxRevisions = 5
                        }), new RavenJObject());
                    using (var session = store.OpenSession("Source"))
                    {
                        var doc = new User { Id = "worker/1", Age = 20 };
                        session.Store(doc);
                        session.SaveChanges();
                    }
                    using (var session = store.OpenSession("Source"))
                    {
                        var doc = session.Load<User>("worker/1");
                        doc.Age++;
                        session.Store(doc);
                        session.SaveChanges();
                    }
                    var smuggler = new SmugglerDatabaseApi(new SmugglerDatabaseOptions()
                    {
                        ShouldDisableVersioningBundle = false
                    });

                    await smuggler.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions>
                    {
                        ToFile = path,
                        From = new RavenConnectionStringOptions()
                        {
                            Url = store.Url,
                            DefaultDatabase = "Source"
                        }
                    });

                    var smuggler2 = new SmugglerDatabaseApi(new SmugglerDatabaseOptions()
                    {
                        ShouldDisableVersioningBundle = false
                    });

                    var e = await AssertAsync.Throws<OperationVetoedException>(async () => await smuggler2.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions>
                    {
                        FromFile = path,
                        To = new RavenConnectionStringOptions()
                        {
                            Url = store.Url,
                            DefaultDatabase = "Dest"
                        }
                    }));

                    Assert.Contains("PUT vetoed on document worker/1/revisions/1 by Raven.Bundles.Versioning.Triggers.VersioningPutTrigger because: Creating a historical revision is not allowed", e.Message);

                }
            }
        }

        [Fact]
        public async Task SmugglingResivionDocsIntoDatabaseWithVersioningDisabled()
        {
            using (var server = GetNewServer())
            {
                var path = Path.GetTempFileName();
                using (var store = NewRemoteDocumentStore(ravenDbServer: server))
                {
                    store
                        .DatabaseCommands
                        .GlobalAdmin
                        .CreateDatabase(new DatabaseDocument
                        {
                            Id = "Source",
                            Settings =
                            {
                                {Constants.ActiveBundles, "Versioning"},
                                {"Raven/DataDir", NewDataPath()}
                            }
                        });
                    store.DatabaseCommands.EnsureDatabaseExists("Source");

                    store
                        .DatabaseCommands
                        .GlobalAdmin
                        .CreateDatabase(new DatabaseDocument
                        {
                            Id = "Dest",
                            Settings =
                            {
                                {Constants.ActiveBundles, "Versioning"},
                                {"Raven/DataDir", NewDataPath()}
                            }
                        });
                    store.DatabaseCommands.EnsureDatabaseExists("Dest");


                    store.DatabaseCommands.ForDatabase("Source")
                        .Put("Raven/Versioning/DefaultConfiguration", null, RavenJObject.FromObject(new Raven.Bundles.Versioning.Data.VersioningConfiguration
                        {
                            Exclude = false,
                            Id = "DefaultConfiguration",
                            MaxRevisions = 5
                        }), new RavenJObject());
                    store.DatabaseCommands.ForDatabase("Dest")
                        .Put("Raven/Versioning/DefaultConfiguration", null, RavenJObject.FromObject(new Raven.Bundles.Versioning.Data.VersioningConfiguration
                        {
                            Exclude = false,
                            Id = "DefaultConfiguration",
                            MaxRevisions = 5
                        }), new RavenJObject());
                    using (var session = store.OpenSession("Source"))
                    {
                        var doc = new User { Id = "worker/1", Age = 20 };
                        session.Store(doc);
                        session.SaveChanges();
                    }
                    using (var session = store.OpenSession("Source"))
                    {
                        var doc = session.Load<User>("worker/1");
                        doc.Age++;
                        session.Store(doc);
                        session.SaveChanges();
                    }
                    var smuggler = new SmugglerDatabaseApi(new SmugglerDatabaseOptions()
                    {
                        ShouldDisableVersioningBundle = false
                    });

                    await smuggler.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions>
                    {
                        ToFile = path,
                        From = new RavenConnectionStringOptions()
                        {
                            Url = store.Url,
                            DefaultDatabase = "Source"
                        }
                    });
                    var smuggler2 = new SmugglerDatabaseApi(new SmugglerDatabaseOptions()
                    {
                        ShouldDisableVersioningBundle = true
                    });
                    var isImporting = await AssertAsync.DoesNotThrow(async () => await smuggler2.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions>
                    {
                        FromFile = path,
                        To = new RavenConnectionStringOptions()
                        {
                            Url = store.Url,
                            DefaultDatabase = "Dest"
                        }
                    }));

                    Assert.True(isImporting);
                }
            }
        }

        public class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
        }
    }
}