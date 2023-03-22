using System.Collections.Generic;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;
using Raven.Client.Documents.Session;

namespace SlowTests.Issues
{
    public class RavenDB_17046 : RavenTestBase
    {
        private const int CollectionSize = 123;
        private readonly List<Category> _categories;
        private readonly List<Company> _companies;

        public RavenDB_17046(ITestOutputHelper output) : base(output)
        {
            _categories = new List<Category>();
            _companies = new List<Company>();

            for (int i = 0; i < CollectionSize; ++i)
            {
                _companies.Add(new Company()
                {
                    Id = $"companies/{i}-A"
                });
                _categories.Add(new Category()
                {
                    Id = $"categories/{i}-A"
                });
            }

            _companies.Sort(CompaniesSortById);
        }

        [Theory]
        [InlineData("c")]
        [InlineData("co")]
        public void CheckIfPagingWorksForUnboundedSet(string startsWithParam)
        {
            var result = Act<Company>("Companies", startsWithParam);
            result.Sort(CompaniesSortById);

            Assert.Equal(CollectionSize, result.Count);
            for (int i = 0; i < CollectionSize; ++i)
                Assert.Equal(_companies[i].Id, result[i].Id);
        }

        [Fact]
        public void CheckIfPagingWorksForUnboundedSetOnAllDocs()
        {
            var result = Act<dynamic>("@all_docs", "c");

            Assert.Equal(result.Count, 2 * CollectionSize);
        }

        private List<T> Act<T>(string collection, string startsWithParam)
        {
            string fullStatement = $"from {collection} where startsWith(id(),'{startsWithParam}')";
            List<T> wholeCollectionByPaging;

            using (var store = GetDocumentStore())
            {
                PrepareDatabaseForTest(store);
                using (var session = store.OpenSession())
                {
                    wholeCollectionByPaging = new List<T>();
                    IList<T> results;
                    int pageNumber = 0;
                    int pageSize = 101;
                    long skippedResults = 0;
                    do
                    {
                        var queryWithPaging = $"{fullStatement} limit {(pageNumber * pageSize) + skippedResults},{pageSize}";

                        results = session
                            .Advanced
                            .RawQuery<T>(queryWithPaging)
                            .Statistics(out QueryStatistics stats)
                            .ToList();
                        wholeCollectionByPaging.AddRange(results);
                        skippedResults += stats.SkippedResults;
                        pageNumber++;

                        if (results.Count == pageSize)
                        {
                            Assert.Equal(-1, stats.TotalResults);
                        }
                    }
                    while (results.Count > 0);
                }
            }

            return wholeCollectionByPaging;
        }

        private void PrepareDatabaseForTest(DocumentStore store)
        {
            using (var bulkInsert = store.BulkInsert())
            {
                for (int i = 0; i < CollectionSize; ++i)
                {
                    bulkInsert.Store(_categories[i]);
                    bulkInsert.Store(_companies[i]);
                }
            }
        }
        private static int CompaniesSortById(Company a, Company b)
        {
            return a.Id.CompareTo(b.Id);
        }
    }
}
