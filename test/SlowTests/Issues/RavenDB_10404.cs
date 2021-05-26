using System;
using System.IO;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10404 : RavenTestBase
    {
        public RavenDB_10404(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanMigrateDatabaseFromFirstStable()
        {
            var backupPath = NewDataPath(forceCreateDir: true);
            var fullBackupPath = Path.Combine(backupPath, "northwind.ravendb-snapshot");

            ExtractFile(fullBackupPath);

            using (var store = GetDocumentStore())
            {
                var databaseName = GetDatabaseName();

                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = backupPath,
                    DatabaseName = databaseName
                }))
                {
                    var stats = store.Maintenance.ForDatabase(databaseName).Send(new GetStatisticsOperation());

                    Assert.Equal(3, stats.CountOfIndexes);
                    Assert.Equal(1057, stats.CountOfDocuments);
                    Assert.Equal(4647, stats.CountOfRevisionDocuments);
                    Assert.Equal(2, stats.CountOfTombstones);
                    Assert.Equal(0, stats.CountOfDocumentsConflicts);
                    Assert.Equal(0, stats.CountOfConflicts);
                    Assert.Equal(17, stats.CountOfAttachments);
                    Assert.Equal(17, stats.CountOfUniqueAttachments);
                    Assert.Equal(3, stats.Indexes.Length);

                    foreach (var indexInformation in stats.Indexes)
                    {
                        var indexStats = store.Maintenance.ForDatabase(databaseName).Send(new GetIndexStatisticsOperation(indexInformation.Name));
                        var indexDefinition = store.Maintenance.ForDatabase(databaseName).Send(new GetIndexOperation(indexInformation.Name));

                        using (var session = store.OpenSession(databaseName))
                        {
                            var results = session.Query<dynamic>(indexInformation.Name).ToList();
                            Assert.Equal(indexStats.EntriesCount, results.Count);
                        }

                        Assert.False(indexStats.IsStale);
                        Assert.False(indexStats.IsInvalidIndex);
                        Assert.Equal(IndexRunningStatus.Running, indexStats.Status);
                        Assert.Equal(IndexState.Normal, indexStats.State);
                        Assert.Equal(IndexPriority.Normal, indexStats.Priority);
                        Assert.Equal(0, indexStats.ErrorsCount);
                        Assert.Equal(0, indexStats.MapErrors);

                        switch (indexStats.Name)
                        {
                            case "Orders/ByCompany":
                                Assert.Equal(89, indexStats.EntriesCount);

                                Assert.Equal(830, indexStats.MapAttempts);
                                Assert.Equal(830, indexStats.MapSuccesses);

                                Assert.Equal(863, indexStats.ReduceAttempts);
                                Assert.Equal(863, indexStats.ReduceSuccesses);

                                Assert.Equal(1, indexStats.MaxNumberOfOutputsPerDocument);
                                Assert.Equal(IndexType.MapReduce, indexStats.Type);

                                Assert.Equal("from result in results\r\ngroup result by result.Company \r\ninto g\r\nselect new\r\n{\r\n\tCompany = g.Key,\r\n\tCount = g.Sum(x => x.Count),\r\n\tTotal = g.Sum(x => x.Total)\r\n}", indexDefinition.Reduce);
                                Assert.Equal(1, indexDefinition.Maps.Count);
                                Assert.Equal("from order in docs.Orders\r\nselect new\r\n{\r\n    order.Company,\r\n    Count = 1,\r\n    Total = order.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))\r\n}", indexDefinition.Maps.First());

                                Assert.Null(indexDefinition.OutputReduceToCollection);

                                Assert.Equal(0, indexDefinition.Fields.Count);
                                break;

                            case "Orders/Totals":
                                Assert.Equal(828, indexStats.EntriesCount);

                                Assert.Equal(830, indexStats.MapAttempts);
                                Assert.Equal(830, indexStats.MapSuccesses);

                                Assert.Null(indexStats.ReduceAttempts);
                                Assert.Null(indexStats.ReduceSuccesses);

                                Assert.Equal(1, indexStats.MaxNumberOfOutputsPerDocument);
                                Assert.Equal(IndexType.Map, indexStats.Type);

                                Assert.Equal(1, indexDefinition.Maps.Count);
                                Assert.Equal("from order in docs.Orders\r\nselect new\r\n{\r\n    order.Employee,\r\n    order.Company,\r\n    Total = order.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))\r\n}", indexDefinition.Maps.First());

                                Assert.Null(indexDefinition.OutputReduceToCollection);

                                Assert.Equal(0, indexDefinition.Fields.Count);
                                break;

                            case "Product/Search":
                                Assert.Equal(77, indexStats.EntriesCount);

                                Assert.Equal(77, indexStats.MapAttempts);
                                Assert.Equal(77, indexStats.MapSuccesses);

                                Assert.Null(indexStats.ReduceAttempts);
                                Assert.Null(indexStats.ReduceSuccesses);

                                Assert.Equal(1, indexStats.MaxNumberOfOutputsPerDocument);
                                Assert.Equal(IndexType.Map, indexStats.Type);

                                Assert.Equal(1, indexDefinition.Maps.Count);
                                Assert.Equal("from p in docs.Products\r\nselect new\r\n{\r\n    p.Name,\r\n    p.Category,\r\n    p.Supplier,\r\n    p.PricePerUnit\r\n}", indexDefinition.Maps.First());

                                Assert.Null(indexDefinition.OutputReduceToCollection);

                                Assert.Equal(1, indexDefinition.Fields.Count);

                                var field = indexDefinition.Fields["Name"];
                                Assert.Null(field.Analyzer);
                                Assert.Equal(FieldIndexing.Search, field.Indexing);
                                Assert.Null(field.Storage);
                                Assert.True(field.Suggestions);
                                Assert.Equal(FieldTermVector.Yes, field.TermVector);
                                Assert.Null(field.Spatial);
                                break;
                            default:
                                throw new InvalidOperationException(indexStats.Name);
                        }
                    }
                }
            }
        }

        private static void ExtractFile(string path)
        {
            using (var file = File.Create(path))
            using (var stream = typeof(RavenDB_10404).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_10404.northwind.4.0.0.ravendb-snapshot"))
            {
                stream.CopyTo(file);
            }
        }
    }
}
