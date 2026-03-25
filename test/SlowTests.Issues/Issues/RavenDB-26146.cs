using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Highlighting;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_26146 : RavenTestBase
    {
        public RavenDB_26146(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Highlighting | RavenTestCategory.Corax)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void Highlighting_with_two_range_filters(Options options)
        {
            using var store = GetDocumentStore(options);
            new SearchCallLogsIndex().Execute(store);

            var start = new DateTimeOffset(2025, 01, 01, 0, 0, 0, TimeSpan.Zero);
            var end = start.AddDays(10);

            using (var session = store.OpenSession())
            {
                session.Store(new CallLog
                {
                    CreatedOn = start.AddDays(2),
                    TranscribeText = "hello world, something to highlight"
                });

                session.Store(new CallLog
                {
                    CreatedOn = start.AddDays(20), // outside range
                    TranscribeText = "hello world"
                });

                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var tagsToUse = new HighlightingOptions
                {
                    PreTags = new[] { "`" },
                    PostTags = new[] { "`" }
                };

                var results = session
                    .Query<CallLog, SearchCallLogsIndex>()
                    .Where(x => x.CreatedOn >= start)
                    .Where(x => x.CreatedOn <= end)
                    .Search(x => x.TranscribeText, "hello", 100)
                    .Highlight(x => x.TranscribeText, 18, 8, tagsToUse, out var highlights)
                    .ToList();

                Assert.Equal(1, results.Count);

                var id = session.Advanced.GetDocumentId(results[0]);
                var frags = highlights.GetFragments(id);

                Assert.NotNull(frags);
                Assert.Equal(1, frags.Length);
                Assert.Equal("`hello` world,", frags[0]);
            }
        }

        private class CallLog
        {
            public string Id { get; set; }
            public DateTimeOffset CreatedOn { get; set; }
            public string TranscribeText { get; set; }
        }

        private class SearchCallLogsIndex : AbstractIndexCreationTask<CallLog>
        {
            public SearchCallLogsIndex()
            {
                Map = docs => from d in docs
                    select new
                    {
                        d.CreatedOn,
                        d.TranscribeText
                    };

                Index(x => x.TranscribeText, FieldIndexing.Search);
                TermVector(x => x.TranscribeText, FieldTermVector.WithPositionsAndOffsets);
                Store(x => x.TranscribeText, FieldStorage.Yes);

                Index(x => x.CreatedOn, FieldIndexing.Default);
                Store(x => x.CreatedOn, FieldStorage.Yes);
            }
        }
    }
}
