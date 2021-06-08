using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Basic.Entities;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Sparrow;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Smuggler
{
    public class LegacySmugglerTests : RavenTestBase
    {
        public LegacySmugglerTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData("SlowTests.Smuggler.Data.Northwind_3.5.35168.ravendbdump")]
        public async Task CanImportNorthwind(string file)
        {
            using (var stream = GetType().Assembly.GetManifestResourceStream(file))
            using (var store = GetDocumentStore())
            {
                Assert.NotNull(stream);

                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());

                Assert.Equal(1059, stats.CountOfDocuments);
                Assert.Equal(3, stats.CountOfIndexes); // there are 4 in ravendbdump, but Raven/DocumentsByEntityName is skipped

                var collectionStats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
                Assert.Equal(1059, collectionStats.CountOfDocuments);
                Assert.Equal(9, collectionStats.Collections.Count);
                Assert.Equal(8, collectionStats.Collections["Categories"]);
                Assert.Equal(91, collectionStats.Collections["Companies"]);
                Assert.Equal(9, collectionStats.Collections["Employees"]);
                Assert.Equal(830, collectionStats.Collections["Orders"]);
                Assert.Equal(77, collectionStats.Collections["Products"]);
                Assert.Equal(4, collectionStats.Collections["Regions"]);
                Assert.Equal(3, collectionStats.Collections["Shippers"]);
                Assert.Equal(29, collectionStats.Collections["Suppliers"]);
                Assert.Equal(8, collectionStats.Collections["@hilo"]);

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>("orders/1");
                    Assert.NotNull(order);

                    var metadata = session.Advanced.GetMetadataFor(order);
                    Assert.False(metadata.ContainsKey("Raven-Entity-Name"));
                    Assert.False(metadata.ContainsKey("Raven-Last-Modified"));
                    Assert.False(metadata.ContainsKey("Last-Modified"));
                }
            }
        }

        [Theory]
        [InlineData("SlowTests.Smuggler.Data.Indexes_And_Transformers_3.5.ravendbdump")]
        public async Task CanImportIndexesAndTransformers(string file)
        {
            using (var stream = GetType().Assembly.GetManifestResourceStream(file))
            using (var store = GetDocumentStore())
            {
                Assert.NotNull(stream);

                await store.Maintenance.SendAsync(new StopIndexingOperation());

                var progresses = new List<IOperationProgress>();
                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
                operation.OnProgressChanged += progress =>
                {
                    progresses.Add(progress);
                };

                var result = operation.WaitForCompletion<SmugglerResult>(TimeSpan.FromMinutes(15));

                Assert.True(progresses.Count > 0);

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.True(record.Indexes.Count > 0);

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());

                Assert.Equal(0, stats.CountOfDocuments);

                // not everything can be imported
                // LoadDocument(key)
                // Spatial

                var unexpectedErrors = new List<string>();

                foreach (var errorMessage in result.Messages)
                {
                    if (errorMessage.Contains("ERROR") == false)
                        continue;

                    if (errorMessage.Contains("No overload for method 'LoadDocument' takes 1 arguments"))
                    {
                        // this.LoadDocument(student.Friends)
                        continue;
                    }

                    if (errorMessage.Contains("The name 'AbstractIndexCreationTask' does not exist in the current context"))
                    {
                        // AbstractIndexCreationTask.SpatialGenerate(
                        continue;
                    }

                    if (errorMessage.Contains("Cannot find analyzer type"))
                    {
                        continue;
                    }

                    if (errorMessage.Contains("The name 'SpatialIndex' does not exist in the current context"))
                    {
                        // SpatialIndex.Generate()
                        continue;
                    }

                    if (errorMessage.Contains("Function cannot contain '__document_id'"))
                    {
                        // __document_id
                        continue;
                    }

                    unexpectedErrors.Add(errorMessage);
                }

                Assert.True(stats.CountOfIndexes >= 463, $"{stats.CountOfIndexes} >= 463. Errors: {string.Join($", {Environment.NewLine}", unexpectedErrors)}");
                Assert.True(stats.CountOfIndexes <= 658, $"{stats.CountOfIndexes} <= 658");

                // not everything can be imported
                // LoadDocument(key)
                // Query
                // QueryOrDefault
            }
        }

        [Theory]
        [InlineData("SlowTests.Smuggler.Data.Revisions_3.5.35220.ravendbdump")]
        public async Task CanImportRevisions1(string file)
        {
            using (var stream = GetType().Assembly.GetManifestResourceStream(file))
            using (var store = GetDocumentStore())
            {
                Assert.NotNull(stream);

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(2, stats.CountOfDocuments);
                Assert.Equal(5, stats.CountOfRevisionDocuments);
                Assert.Equal(10, stats.LastDocEtag);

                var collectionStats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
                Assert.Equal(2, collectionStats.CountOfDocuments);
                Assert.Equal(2, collectionStats.Collections.Count);
                Assert.Equal(1, collectionStats.Collections["@empty"]);
                Assert.Equal(1, collectionStats.Collections["Users"]);

                using (var session = store.OpenSession())
                {
                    var user = session.Load<Order>("users/1");
                    Assert.NotNull(user);

                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal(5, metadata.Count);
                    Assert.Equal("Users", metadata.GetString(Constants.Documents.Metadata.Collection));
                    Assert.Equal($"{DocumentFlags.HasRevisions}", metadata.GetString(Constants.Documents.Metadata.Flags));
                    Assert.Equal("users/1", metadata.GetString(Constants.Documents.Metadata.Id));
                    Assert.NotEqual(DateTime.MinValue.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite), metadata.GetString(Constants.Documents.Metadata.LastModified));

                    var changeVector = session.Advanced.GetChangeVectorFor(user);
                    Assert.StartsWith("RV:", changeVector);

                    var revisions = session.Advanced.Revisions.GetFor<User>("users/1");
                    Assert.Equal(4, revisions.Count);

                    for (int i = 0; i <= 3; i++)
                    {
                        metadata = session.Advanced.GetMetadataFor(revisions[i]);
                        Assert.Equal(5, metadata.Count);
                        Assert.Equal("Users", metadata.GetString(Constants.Documents.Metadata.Collection));
                        Assert.Equal($"{DocumentFlags.HasRevisions}, {DocumentFlags.Revision}", metadata.GetString(Constants.Documents.Metadata.Flags));
                        Assert.Equal("users/1", metadata.GetString(Constants.Documents.Metadata.Id));
                        Assert.NotEqual(DateTime.MinValue.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite), metadata.GetString(Constants.Documents.Metadata.LastModified));

                        var revisionChangeVector = metadata.GetString(Constants.Documents.Metadata.ChangeVector);
                        Assert.Equal($"RV:{4 - i}-AAAAAQAAAQAAAAAAAAAAAw", revisionChangeVector);

                        if (i == 0)
                            Assert.Equal(changeVector, revisionChangeVector);
                    }
                }
            }
        }

        [Theory]
        [InlineData("SlowTests.Smuggler.Data.DocumentWithRevisions.ravendbdump")]
        public async Task CanImportRevisions2(string file)
        {
            using (var stream = GetType().Assembly.GetManifestResourceStream(file))
            using (var store = GetDocumentStore())
            {
                Assert.NotNull(stream);

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(2, stats.CountOfDocuments);
                Assert.Equal(6, stats.CountOfRevisionDocuments);
                Assert.Equal(11, stats.LastDocEtag);

                var collectionStats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
                Assert.Equal(2, collectionStats.CountOfDocuments);
                Assert.Equal(2, collectionStats.Collections.Count);
                Assert.Equal(1, collectionStats.Collections["@empty"]);
                Assert.Equal(1, collectionStats.Collections["test"]);

                using (var session = store.OpenSession())
                {
                    var test = session.Load<Test>("test");
                    Assert.NotNull(test);
                    Assert.Equal("4", test.Name);
                    var changeVector = session.Advanced.GetChangeVectorFor(test);

                    var revisions = session.Advanced.Revisions.GetFor<User>("test");

                    Assert.Equal("4", revisions[0].Name);
                    Assert.Equal("3", revisions[1].Name);
                    Assert.Equal("2", revisions[2].Name);
                    Assert.Equal("1", revisions[3].Name);
                    Assert.Equal("...", revisions[4].Name);

                    foreach (var revision in revisions)
                    {
                        var metadata = session.Advanced.GetMetadataFor(revision);
                        Assert.Equal($"{DocumentFlags.HasRevisions}, {DocumentFlags.Revision}", metadata.GetString(Constants.Documents.Metadata.Flags));
                    }

                    var revisionChangeVector = session.Advanced.GetChangeVectorFor(revisions[0]);
                    Assert.Equal(changeVector, revisionChangeVector);
                }
            }
        }

        [Theory]
        [InlineData("SlowTests.Smuggler.Data.RevisionsWithoutADocument.ravendbdump")]
        public async Task CanImportRevisionsWithoutADocument(string file)
        {
            using (var stream = GetType().Assembly.GetManifestResourceStream(file))
            using (var store = GetDocumentStore())
            {
                Assert.NotNull(stream);

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(1, stats.CountOfDocuments);
                Assert.Equal(7, stats.CountOfRevisionDocuments);
                Assert.Equal(2, stats.LastDocEtag);

                var collectionStats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
                Assert.Equal(1, collectionStats.CountOfDocuments);
                Assert.Equal(2, collectionStats.Collections.Count);
                Assert.Equal(1, collectionStats.Collections["@empty"]);
                Assert.Equal(0, collectionStats.Collections["test"]);

                using (var session = store.OpenSession())
                {
                    var test = session.Load<Test>("test");
                    Assert.Null(test);

                    var revisions = session.Advanced.Revisions.GetFor<User>("test");
                    Assert.Equal(6, revisions.Count);

                    var metadata = session.Advanced.GetMetadataFor(revisions[0]);
                    Assert.Equal($"{DocumentFlags.HasRevisions}, {DocumentFlags.DeleteRevision}", metadata.GetString(Constants.Documents.Metadata.Flags));

                    Assert.Equal("4", revisions[1].Name);
                    Assert.Equal("3", revisions[2].Name);
                    Assert.Equal("2", revisions[3].Name);
                    Assert.Equal("1", revisions[4].Name);
                    Assert.Equal("...", revisions[5].Name);
                }
            }
        }

        [Theory]
        [InlineData("SlowTests.Smuggler.Data.RevisionsWithoutADocument.ravendbdump")]
        public async Task CanImportRevisionsWithoutADocumentWithPurgeOnDelete(string file)
        {
            using (var stream = GetType().Assembly.GetManifestResourceStream(file))
            using (var store = GetDocumentStore())
            {
                Assert.NotNull(stream);

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, x => x.Default.PurgeOnDelete = true);

                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(1, stats.CountOfDocuments);
                Assert.Equal(1, stats.CountOfRevisionDocuments);
                Assert.Equal(2, stats.LastDocEtag);

                var collectionStats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
                Assert.Equal(1, collectionStats.CountOfDocuments);
                Assert.Equal(2, collectionStats.Collections.Count);
                Assert.Equal(1, collectionStats.Collections["@empty"]);
                Assert.Equal(0, collectionStats.Collections["test"]);

                using (var session = store.OpenSession())
                {
                    var test = session.Load<Test>("test");
                    Assert.Null(test);

                    var revisions = session.Advanced.Revisions.GetFor<User>("test");
                    Assert.Equal(0, revisions.Count);
                }
            }
        }

        [Theory]
        [InlineData("SlowTests.Smuggler.Data.Identities_3.5.35288.ravendbdump")]
        public async Task CanImportIdentities(string file)
        {
            using (var stream = GetType().Assembly.GetManifestResourceStream(file))
            using (var store = GetDocumentStore())
            {
                Assert.NotNull(stream);

                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(2, stats.CountOfDocuments);

                var collectionStats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
                Assert.Equal(2, collectionStats.Collections["Users"]);

                using (var session = store.OpenSession())
                {
                    var user = new User();
                    session.Store(user, "users|");
                    session.SaveChanges();

                    Assert.Equal("users/3", user.Id);
                }

                collectionStats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
                Assert.Equal(3, collectionStats.Collections["Users"]);
            }
        }

        private class User
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public string Version { get; set; }
        }

        private class Test
        {
            public string Name { get; set; }
        }
    }
}
