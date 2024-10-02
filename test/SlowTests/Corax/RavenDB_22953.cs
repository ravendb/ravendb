using System;
using System.Globalization;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class RavenDB_22953 : RavenTestBase
{
    public RavenDB_22953(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void StreamingQueryMustNotThrowIndexOutOfRangeException(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var timestamps = new[]
            {
                new DateTimeOffset(new DateTime(2024, 9, 2), TimeSpan.Zero),
                new DateTimeOffset(new DateTime(2024, 9, 3), TimeSpan.Zero),
                new DateTimeOffset(new DateTime(2024, 9, 5), TimeSpan.Zero),
                new DateTimeOffset(new DateTime(2024, 9, 12), TimeSpan.Zero),
                new DateTimeOffset(new DateTime(2024, 9, 13), TimeSpan.Zero),
                new DateTimeOffset(new DateTime(2024, 9, 15), TimeSpan.Zero),
                new DateTimeOffset(new DateTime(2024, 9, 17), TimeSpan.Zero),
                new DateTimeOffset(new DateTime(2024, 9, 20), TimeSpan.Zero),
                new DateTimeOffset(new DateTime(2024, 9, 21), TimeSpan.Zero),
                new DateTimeOffset(new DateTime(2024, 9, 22), TimeSpan.Zero),
                new DateTimeOffset(new DateTime(2024, 9, 22), TimeSpan.Zero),
                new DateTimeOffset(new DateTime(2024, 9, 27), TimeSpan.Zero),
            };

            using (var bulk = store.BulkInsert())
            {
                for (int i = 0; i < 10000; i++)
                {
                    bulk.Store(new Entry
                    {
                        Timestamp = timestamps[i % timestamps.Length],
                        Schema = "foo",
                        Name = "bar"
                    });
                }
            }

            using (var bulk = store.BulkInsert())
            {
                for (int i = 0; i < 11000; i++)
                {
                    bulk.Store(new Entry
                    {
                        Timestamp = timestamps[i % timestamps.Length],
                        Schema = "zzz",
                        Name = "bar"
                    });
                }
            }

            new Entries_ByTimestampAndQualifiedName().Execute(store);

            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var count = 0;

                var q = session.Advanced.DocumentQuery<Entries_ByTimestampAndQualifiedName.IndexEntry, Entries_ByTimestampAndQualifiedName>()
                    .WhereGreaterThanOrEqual(x => x.Timestamp, new DateTimeOffset(new DateTime(2024, 9, 2), TimeSpan.Zero))
                    .WhereLessThanOrEqual(x => x.Timestamp, new DateTimeOffset(new DateTime(2024, 9, 27), TimeSpan.Zero))
                    .WhereIn(x => x.QualifiedName, new[] { "foo:bar" })
                    .OrderBy(x => x.Timestamp);

                var s = session.Advanced.Stream(q);

                while (s.MoveNext())
                {
                    count++;
                }

                Assert.Equal(10000, count);
            }
        }
    }

    private class Entry
    {
        public DateTimeOffset? Timestamp { get; set; }

        public string Name { get; set; }

        public string Schema { get; set; }
    }

    private class Entries_ByTimestampAndQualifiedName : AbstractIndexCreationTask<Entry>
    {
        public class IndexEntry
        {
            public DateTimeOffset Timestamp { get; set; }

            public string QualifiedName { get; set; }
        }

        public Entries_ByTimestampAndQualifiedName()
        {
            Map = entries => from entry in entries
                select new IndexEntry
                {
                    Timestamp = entry.Timestamp ??
                                default(DateTimeOffset),
                    QualifiedName = string.Format("{0}:{1}", entry.Schema, entry.Name),
                };
        }
    }
}
