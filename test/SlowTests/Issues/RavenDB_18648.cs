using System;
using System.IO;
using FastTests;
using Orders;
using Raven.Client.Documents.Operations;
using SlowTests.Core.Utils.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18648 : RavenTestBase
{
    public RavenDB_18648(ITestOutputHelper output) : base(output)
    {
    }

    [Theory]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public void Can_Get_Basic_Database_Statistics(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var stats = store.Maintenance.Send(new GetBasicStatisticsOperation());

            Assert.Equal(0, stats.CountOfAttachments);
            Assert.Equal(0, stats.CountOfConflicts);
            Assert.Equal(0, stats.CountOfCounterEntries);
            Assert.Equal(0, stats.CountOfDocuments);
            Assert.Equal(0, stats.CountOfDocumentsConflicts);
            Assert.Equal(0, stats.CountOfIndexes);
            Assert.Equal(0, stats.CountOfRevisionDocuments);
            Assert.Equal(0, stats.CountOfTimeSeriesSegments);
            Assert.Equal(0, stats.CountOfTombstones);
            Assert.Equal(0, stats.Indexes.Length);

            using (var session = store.OpenSession())
            {
                for (var i = 0; i < 20; i++)
                {
                    var company = new Company();
                    session.Store(company);
                    session.TimeSeriesFor(company, "TS").Append(DateTime.Now, 1);
                    session.CountersFor(company).Increment("CTR", 1);
                    session.Advanced.Attachments.Store(company, "a1", new MemoryStream());
                }

                session.SaveChanges();
            }

            var index = new Companies_SortByName();
            index.Execute(store);

            stats = store.Maintenance.Send(new GetBasicStatisticsOperation());

            Assert.Equal(20, stats.CountOfAttachments);
            Assert.Equal(0, stats.CountOfConflicts);
            Assert.Equal(20, stats.CountOfCounterEntries);
            Assert.Equal(21, stats.CountOfDocuments);
            Assert.Equal(0, stats.CountOfDocumentsConflicts);
            Assert.Equal(1, stats.CountOfIndexes);
            Assert.Equal(0, stats.CountOfRevisionDocuments);
            Assert.Equal(20, stats.CountOfTimeSeriesSegments);
            Assert.Equal(0, stats.CountOfTombstones);
            Assert.Equal(1, stats.Indexes.Length);

            var indexInformation = stats.Indexes[0];
            Assert.Equal(index.IndexName, indexInformation.Name);
        }
    }
}
