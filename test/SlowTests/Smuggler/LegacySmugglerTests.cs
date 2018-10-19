using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Basic.Entities;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Sparrow;
using Xunit;

namespace SlowTests.Smuggler
{
    public class LegacySmugglerTests : RavenTestBase
    {
        [Theory]
        [InlineData("SlowTests.Smuggler.Northwind_3.5.35168.ravendbdump")]
        public async Task CanImportNorthwind(string file)
        {
            using (var stream = GetType().GetTypeInfo().Assembly.GetManifestResourceStream(file))
            using (var store = GetDocumentStore())
            {
                Assert.NotNull(stream);

                await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);

                var stats = await GetStatsAsync(store, s => s.CountOfIndexes >= 3, TimeSpan.FromSeconds(10));

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
        [InlineData("SlowTests.Smuggler.Indexes_And_Transformers_3.5.ravendbdump")]
        public async Task CanImportIndexesAndTransformers(string file)
        {
            using (var stream = GetType().GetTypeInfo().Assembly.GetManifestResourceStream(file))
            using (var store = GetDocumentStore())
            {
                Assert.NotNull(stream);

                await store.Maintenance.SendAsync(new StopIndexingOperation());

                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);

                var result = operation.WaitForCompletion<SmugglerResult>();

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.True(record.Indexes.Count > 0);

                var stats = await GetStatsAsync(store, s => s.CountOfIndexes >= 463, TimeSpan.FromSeconds(60));

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
        [InlineData("SlowTests.Smuggler.Revisions_3.5.35220.ravendbdump")]
        public async Task CanImportRevisions(string file)
        {
            using (var stream = GetType().GetTypeInfo().Assembly.GetManifestResourceStream(file))
            using (var store = GetDocumentStore())
            {
                Assert.NotNull(stream);

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(2, stats.CountOfDocuments);
                Assert.Equal(5, stats.CountOfRevisionDocuments);
                Assert.Equal(6, stats.LastDocEtag);

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
                    Assert.StartsWith("A:6-", metadata.GetString(Constants.Documents.Metadata.ChangeVector));
                    Assert.Equal(DocumentFlags.HasRevisions.ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                    Assert.Equal("users/1", metadata.GetString(Constants.Documents.Metadata.Id));
                    Assert.NotEqual(DateTime.MinValue.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite), metadata.GetString(Constants.Documents.Metadata.LastModified));

                    var revisions = session.Advanced.Revisions.GetFor<User>("users/1");
                    Assert.Equal(4, revisions.Count);

                    for (int i = 0; i <= 3; i++)
                    {
                        metadata = session.Advanced.GetMetadataFor(revisions[i]);
                        Assert.Equal(5, metadata.Count);
                        Assert.Equal("Users", metadata.GetString(Constants.Documents.Metadata.Collection));
                        Assert.Equal($"RV:{4 - i}-AAAAAQAAAQAAAAAAAAAAAw", metadata.GetString(Constants.Documents.Metadata.ChangeVector));
                        Assert.Equal(DocumentFlags.Revision.ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                        Assert.Equal("users/1", metadata.GetString(Constants.Documents.Metadata.Id));
                        Assert.NotEqual(DateTime.MinValue.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite), metadata.GetString(Constants.Documents.Metadata.LastModified));
                    }
                }
            }
        }

        private static async Task<DatabaseStatistics> GetStatsAsync(IDocumentStore store, Func<DatabaseStatistics, bool> action, TimeSpan timeout)
        {
            var sw = Stopwatch.StartNew();
            while (sw.Elapsed < timeout)
            {
                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                if (action(stats))
                    return stats;

                await Task.Delay(1000);
            }

            throw new TimeoutException("Did not get stats in " + timeout);
        }

        private class User
        {
            public string Name { get; set; }
            public string Version { get; set; }
        }
    }
}
