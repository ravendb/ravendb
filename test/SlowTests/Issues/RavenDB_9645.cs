using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.ServerWide;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9645 : RavenTestBase
    {
        public RavenDB_9645(ITestOutputHelper output) : base(output)
        {
        }

        public const int LENGTH_OF_NAME = 2;
        public const string BLOB_OF_DATA = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Integer nec odio. Praesent libero. Sed cursus ante dapibus diam. Sed nisi. Nulla quis sem at nibh elementum imperdiet. Duis sagittis ipsum. Praesent mauris. Fusce nec tellus sed augue semper porta. Mauris massa. Vestibulum lacinia arcu eget nulla. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Curabitur sodales ligula in libero. Sed dignissim lacinia nunc. Curabitur tortor. Pellentesque nibh. Aenean quam. In scelerisque sem at dolor. Maecenas mattis. Sed convallis tristique sem. Proin ut ligula vel nunc egestas porttitor. Morbi lectus risus, iaculis vel, suscipit quis, luctus non, massa. Fusce ac turpis quis ligula lacinia aliquet. Mauris ipsum. Nulla metus metus, ullamcorper vel, tincidunt sed, euismod in, nibh. Quisque volutpat condimentum velit. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Nam nec ante. Sed lacinia, urna non tincidunt mattis, tortor neque adipiscing diam, a cursus ipsum ante quis turpis. Nulla facilisi. Ut fringilla. Suspendisse potenti. Nunc feugiat mi a tellus consequat imperdiet. Vestibulum sapien. Proin quam. Etiam ultrices. Suspendisse in justo eu magna luctus suscipit. Sed lectus. Integer euismod lacus luctus magna. Quisque cursus, metus vitae pharetra auctor, sem massa mattis sem, at interdum magna augue eget diam. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Morbi lacinia molestie dui. Praesent blandit dolor. Sed non quam. In vel mi sit amet augue congue elementum. Morbi in ipsum sit amet pede facilisis laoreet. Donec lacus nunc, viverra nec.";

        [Theory]
        [InlineData(5000, true)]
        [InlineData(5000, false)]
        public void Should_correctly_reduce_after_updating_all_documents(int numberOfClaimsToGenerate, bool compressed)
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    if (compressed)
                    {
                        record.DocumentsCompression = new DocumentsCompressionConfiguration
                        {
                            Collections = new[]{ "Claims" },
                            CompressRevisions = true
                        };
                    }
                }
            }))
            {
                new ClaimsByBillTypeAndMatchingStatus().Execute(store);

                GenerateTestClaims(store, numberOfClaimsToGenerate);

                Indexes.WaitForIndexing(store);

                while (true)
                {
                    using (var claimSession = store.OpenSession())
                    {
                        var query = claimSession.Query<Claim>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Take(100)
                            .Where(c => c.MatchingStatus.Equals("UNKNOWN"))
                            .OrderBy(c => c.ControlNumber).ToList();

                        if (query.Count == 0)
                            break;

                        foreach (var claim in query)
                        {
                            claim.MatchingStatus = "MATCHED";
                        }

                        claimSession.SaveChanges();
                    }

                    Indexes.WaitForIndexing(store);
                }

                Indexes.WaitForIndexing(store);

                Assert.Null(Indexes.WaitForIndexingErrors(store, errorsShouldExists: false));

                using (var session = store.OpenSession())
                {
                    var results = session.Query<ClaimsByBillTypeAndMatchingStatus.Result, ClaimsByBillTypeAndMatchingStatus>().OrderBy(x => x.BillType).ToList();

                    Assert.Equal(2, results.Count);

                    Assert.Equal(numberOfClaimsToGenerate / 2, results[0].Count);
                    Assert.Equal("MATCHED", results[0].MatchingStatus);
                    Assert.Equal("110", results[0].BillType);

                    Assert.Equal(numberOfClaimsToGenerate / 2, results[1].Count);
                    Assert.Equal("MATCHED", results[1].MatchingStatus);
                    Assert.Equal("111", results[1].BillType);
                }

                var operation = store.Operations.Send(new DeleteByQueryOperation(new IndexQuery { Query = "FROM Claims" }));
                operation.WaitForCompletion(TimeSpan.FromSeconds(60));

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<ClaimsByBillTypeAndMatchingStatus.Result, ClaimsByBillTypeAndMatchingStatus>().OrderBy(x => x.BillType).ToList();

                    Assert.Equal(0, results.Count);
                }
            }
        }

        // Used to create a random string for a name
        private static readonly Random random = new Random(1);

        public static string RandomString(int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
            return new string(Enumerable.Repeat(chars, length)
                .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        // Used to create a series of test documents to recreate the issue.
        private void GenerateTestClaims(DocumentStore documentStore, int numberOfClaimsCreate)
        {
            using (var bulk = documentStore.BulkInsert())
            {
                for (int i = 0; i < numberOfClaimsCreate; i++)
                {
                    var trailingBillTypeDigit = (i % 2);

                    var claim = new Claim
                    {
                        Id = Guid.NewGuid().ToString().Replace("-", ""),
                        BillType = $"11{trailingBillTypeDigit}",
                        PatientName = RandomString(LENGTH_OF_NAME),
                        MatchingStatus = "UNKNOWN",
                        Data = BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA + BLOB_OF_DATA
                    };

                    claim.ControlNumber = claim.PatientName;

                    bulk.Store(claim);
                }

            }
        }

        private class ClaimsByBillTypeAndMatchingStatus : AbstractIndexCreationTask<Claim, ClaimsByBillTypeAndMatchingStatus.Result>
        {
            public class Result
            {
                public string MatchingStatus { get; set; }
                public string BillType { get; set; }
                public int Count { get; set; }
            }

            public ClaimsByBillTypeAndMatchingStatus()
            {
                Map = claims => from c in claims
                                select new
                                {
                                    c.MatchingStatus,
                                    c.BillType,
                                    Count = 1
                                };

                Reduce = results => from r in results
                                    group r by new { r.BillType, r.MatchingStatus }
                    into g
                                    let count = g.Sum(x => x.Count)
                                    select new
                                    {
                                        g.Key.MatchingStatus,
                                        BillType = g.Key.BillType,
                                        Count = count
                                    };
            }
        }

        private class Claim
        {
            public string Id { get; set; }
            public string PatientName { get; set; }
            public string MatchingStatus { get; set; }
            public string BillType { get; set; }
            public string Data { get; set; }
            public string ControlNumber { get; set; }
            public string EncounterId { get; set; }
        }
    }
}
