// -----------------------------------------------------------------------
//  <copyright file="RaveDB-4085.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Database.Extensions;
using Raven.Database.Smuggler;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4085 : RavenTest
    {
        [Fact]
        public async Task can_export_all_documents()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (var store = NewDocumentStore())
                {
                    for (var i = 0; i < 1000; i++)
                    {
                        store.DatabaseCommands.Put("users/" + (i + 1), null, new RavenJObject()
                        {
                            { "Name", "test #" + i }
                        }, new RavenJObject()
                        {
                            { Constants.RavenEntityName, "Users"}
                        });
                    }

                    var task1 = Task.Run(async () =>
                    {
                        // now perform full backup
                        var dumper = new DatabaseDataDumper(store.SystemDatabase)
                        {
                            Options =
                            {
                                BatchSize = 10,
                                Incremental = true
                            }
                        };
                        await dumper.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> {ToFile = backupPath});
                    });
                    var task2 = Task.Run(() =>
                    {
                        // change the one document, this document should be exported (any version of it)
                        for (var i = 0; i < 100; i++)
                        {
                            using (var session = store.OpenSession())
                            {
                                var user = session.Load<User>("users/1000");
                                user.Name = "test" + i;
                                session.SaveChanges();
                            }
                        }
                    });

                    await Task.WhenAll(task1, task2);
                }

                using (var embeddableStore = NewDocumentStore())
                {
                    // import all the data
                    var dumper = new DatabaseDataDumper(embeddableStore.SystemDatabase) { Options = { Incremental = true } };
                    dumper.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = backupPath }).Wait();

                    using (var session = embeddableStore.OpenSession())
                    {
                        var user = session.Load<User>("users/1000");
                        //the document should exist in the export (any version of it)
                        Assert.NotNull(user);

                        var list = session.Query<User>()
                            .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                            .Take(1024)
                            .ToList();
                        Assert.Equal(1000, list.Count);
                    }

                    var stats = embeddableStore.DatabaseCommands.GetStatistics();
                    Assert.Equal(1000, stats.CountOfDocuments);
                }
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
            }
        }

        [Fact]
        public async Task can_export_all_attachments()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (var store = NewDocumentStore())
                {
                    for (var i = 0; i < 1000; i++)
                    {
                        store.DatabaseCommands.PutAttachment(
                            "attachments/" + (i + 1),
                            null,
                            new MemoryStream(new[] { (byte)i }),
                            new RavenJObject());
                    }

                    var task1 = Task.Run(async () =>
                    {
                        // now perform full backup
                        var dumper = new DatabaseDataDumper(store.SystemDatabase)
                        {
                            Options =
                            {
                                BatchSize = 10,
                                Incremental = true
                            }
                        };
                        await dumper.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { ToFile = backupPath });
                    });
                    var task2 = Task.Run(() =>
                    {
                        // change the one document, this document should be exported (any version of it)
                        for (var i = 0; i < 100; i++)
                        {
                            store.DatabaseCommands.PutAttachment(
                                "attachments/1000", 
                                null,
                                new MemoryStream(new[] { (byte)i }), 
                                new RavenJObject());
                        }
                    });

                    await Task.WhenAll(task1, task2);
                }

                using (var embeddableStore = NewDocumentStore())
                {
                    // import all the data
                    var dumper = new DatabaseDataDumper(embeddableStore.SystemDatabase) { Options = { Incremental = true } };
                    dumper.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = backupPath }).Wait();

                    var attachemnt = embeddableStore.DatabaseCommands.GetAttachment("attachments/1000");
                    Assert.NotNull(attachemnt);

                    var attachments = embeddableStore.DatabaseCommands.GetAttachments(0, Etag.Empty, 1024).ToList();
                    Assert.Equal(1000, attachments.Count);

                    var stats = embeddableStore.DatabaseCommands.GetStatistics();
                    Assert.Equal(1000, stats.CountOfAttachments);
                }
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
            }
        }
        public class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
