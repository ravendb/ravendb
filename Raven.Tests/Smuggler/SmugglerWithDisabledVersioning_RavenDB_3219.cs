// -----------------------------------------------------------------------
//  <copyright file="SmugglerWithVersioning.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Client.Bundles.Versioning;
using Raven.Database.Smuggler;
using Raven.Json.Linq;
using Raven.Smuggler;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Smuggler
{
    public class SmugglerWithDisabledVersioning_RavenDB_3219 : RavenTest
    {
        [Fact]
        public async Task CanDisableVersioningDuringImport_Remote()
        {
            using (var stream = new MemoryStream())
            {
                long countOfDocuments;
                using (var store = NewRemoteDocumentStore())
                {
                    using (var session = store.OpenSession())
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            session.Store(new User());
                            session.Store(new Address());
                        }

                        session.SaveChanges();
                    }

                    countOfDocuments = store.DatabaseCommands.GetStatistics().CountOfDocuments;

                    var smuggler = new SmugglerDatabaseApi(new SmugglerDatabaseOptions());

                    await smuggler.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions>
                    {
                        ToStream = stream,
                        From = new RavenConnectionStringOptions()
                        {
                            Url = store.Url,
                            DefaultDatabase = store.DefaultDatabase
                        }
                    });
                }

                stream.Position = 0;

                using (var store = NewRemoteDocumentStore(activeBundles: "Versioning"))
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new Bundles.Versioning.Data.VersioningConfiguration
                        {
                            Exclude = false,
                            Id = "Raven/Versioning/DefaultConfiguration",
                            MaxRevisions = 5
                        });

                        session.SaveChanges();
                    }

                    var smuggler = new SmugglerDatabaseApi(new SmugglerDatabaseOptions
                    {
                        ShouldDisableVersioningBundle = true
                    });

                    await smuggler.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions>()
                    {
                        FromStream = stream,
                        To = new RavenConnectionStringOptions()
                        {
                            Url = store.Url,
                            DefaultDatabase = store.DefaultDatabase
                        }
                    });

                    var countOfDocsAfterImport = store.DatabaseCommands.GetStatistics().CountOfDocuments;

                    Assert.Equal(countOfDocuments, countOfDocsAfterImport - 1); // one additional doc for versioning bundle configuration

                    var metadata = store.DatabaseCommands.Get("users/1").Metadata;

                    Assert.True(metadata.ContainsKey(Constants.RavenIgnoreVersioning) == false, "Metadata contains temporary " + Constants.RavenIgnoreVersioning + " marker");

                    // after import versioning should be active
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User(), "users/arek");

                        session.SaveChanges();

                        var revisionsFor = session.Advanced.GetRevisionsFor<User>("users/arek", 0, 10);

                        Assert.Equal(1, revisionsFor.Length);
                    }
                }
            }
        }

        [Fact]
        public async Task CanDisableVersioningDuringImport_Embedded()
        {
            using (var stream = new MemoryStream())
            {
                long countOfDocuments;
                using (var store = NewDocumentStore())
                {
                    using (var session = store.OpenSession())
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            session.Store(new User());
                            session.Store(new Address());
                        }

                        session.SaveChanges();
                    }

                    countOfDocuments = store.DatabaseCommands.GetStatistics().CountOfDocuments;

                    var smuggler = new DatabaseDataDumper(store.DocumentDatabase, new SmugglerDatabaseOptions());

                    await smuggler.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions>
                    {
                        ToStream = stream,
                        From = new RavenConnectionStringOptions()
                        {
                            DefaultDatabase = store.DefaultDatabase
                        }
                    });
                }

                stream.Position = 0;

                using (var store = NewDocumentStore(activeBundles: "Versioning"))
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new Bundles.Versioning.Data.VersioningConfiguration
                        {
                            Exclude = false,
                            Id = "Raven/Versioning/DefaultConfiguration",
                            MaxRevisions = 5
                        });

                        session.SaveChanges();
                    }

                    var smuggler = new DatabaseDataDumper(store.DocumentDatabase, new SmugglerDatabaseOptions
                    {
                        ShouldDisableVersioningBundle = true
                    });

                    await smuggler.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions>()
                    {
                        FromStream = stream,
                        To = new RavenConnectionStringOptions()
                        {
                            DefaultDatabase = store.DefaultDatabase
                        }
                    });

                    var countOfDocsAfterImport = store.DatabaseCommands.GetStatistics().CountOfDocuments;

                    Assert.Equal(countOfDocuments, countOfDocsAfterImport - 1); // one additional doc for versioning bundle configuration

                    var metadata = store.DatabaseCommands.Get("users/1").Metadata;

                    Assert.True(metadata.ContainsKey(Constants.RavenIgnoreVersioning) == false, "Metadata contains temporary " + Constants.RavenIgnoreVersioning + " marker");

                    // after import versioning should be active
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User(), "users/arek");

                        session.SaveChanges();

                        var revisionsFor = session.Advanced.GetRevisionsFor<User>("users/arek", 0, 10);

                        Assert.Equal(1, revisionsFor.Length);
                    }
                }
            }
        }

        [Fact]
        public async Task CanDisableVersioningDuringImport_Between_Remote()
        {
            using (var server = GetNewServer())
            {
                using (var store = NewRemoteDocumentStore(ravenDbServer: server))
                {
                    store
                    .DatabaseCommands
                    .GlobalAdmin
                    .CreateDatabase(new DatabaseDocument
                    {
                        Id = "Import",
                        Settings =
                        {
                            { Constants.ActiveBundles, "Versioning" },
                            { "Raven/DataDir", NewDataPath() }
                        }
                    });

                    using (var session = store.OpenSession())
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            session.Store(new User());
                            session.Store(new Address());
                        }

                        session.SaveChanges();
                    }

                    var countOfDocuments = store.DatabaseCommands.GetStatistics().CountOfDocuments;

                    store.DatabaseCommands.ForDatabase("Import").Put("Raven/Versioning/DefaultConfiguration", null, RavenJObject.FromObject(new Bundles.Versioning.Data.VersioningConfiguration
                    {
                        Exclude = false,
                        Id = "Raven/Versioning/DefaultConfiguration",
                        MaxRevisions = 5
                    }), new RavenJObject());

                    var smuggler = new SmugglerDatabaseApi(new SmugglerDatabaseOptions()
                    {
                        ShouldDisableVersioningBundle = true
                    });

                    await smuggler.Between(new SmugglerBetweenOptions<RavenConnectionStringOptions>()
                    {
                        From = new RavenConnectionStringOptions()
                        {
                            Url = store.Url,
                            DefaultDatabase = store.DefaultDatabase
                        },
                        To = new RavenConnectionStringOptions()
                        {
                            Url = store.Url,
                            DefaultDatabase = "Import"
                        }
                    });

                    var countOfDocsAfterImport = store.DatabaseCommands.ForDatabase("Import").GetStatistics().CountOfDocuments;

                    Assert.Equal(countOfDocuments, countOfDocsAfterImport - 1); // one additional doc for versioning bundle configuration

                    var metadata = store.DatabaseCommands.ForDatabase("Import").Get("users/1").Metadata;

                    Assert.True(metadata.ContainsKey(Constants.RavenIgnoreVersioning) == false, "Metadata contains temporary " + Constants.RavenIgnoreVersioning + " marker");

                    // after import versioning should be active
                    using (var session = store.OpenSession("Import"))
                    {
                        session.Store(new User(), "users/arek");

                        session.SaveChanges();

                        var revisionsFor = session.Advanced.GetRevisionsFor<User>("users/arek", 0, 10);

                        Assert.Equal(1, revisionsFor.Length);
                    }
                }

            }
        }
    }
}
