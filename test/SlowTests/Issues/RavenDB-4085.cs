// -----------------------------------------------------------------------
//  <copyright file="RaveDB-4085.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Utils;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_4085 : RavenTestBase
    {
        [Fact]
        public async Task can_export_all_documents()
        {
            var backupPath = Path.Combine(NewDataPath(forceCreateDir: true), "export.dump");
            try
            {
                using (var store = GetDocumentStore())
                {
                    using (var commands = store.Commands())
                    {
                        for (var i = 0; i < 1000; i++)
                        {
                            commands.Put("users/" + (i + 1), null, new { Name = "test #" + i }, new Dictionary<string, object>
                            {
                                { Constants.Documents.Metadata.Collection, "Users" }
                            });
                        }
                    }

                    var task1 = Task.Run(async () =>
                    {
                        // now perform full backup
                        var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), backupPath);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
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

                using (var store = GetDocumentStore())
                {
                    // import all the data
                    var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), backupPath);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    using (var session = store.OpenSession())
                    {
                        var user = session.Load<User>("users/1000");
                        //the document should exist in the export (any version of it)
                        Assert.NotNull(user);

                        var list = session.Query<User>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Take(1024)
                            .ToList();
                        Assert.Equal(1000, list.Count);
                    }

                    var stats = store.Maintenance.Send(new GetStatisticsOperation());
                    Assert.Equal(1000, stats.CountOfDocuments);
                }
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
