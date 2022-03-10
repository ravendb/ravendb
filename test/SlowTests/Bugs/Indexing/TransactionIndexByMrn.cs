using FastTests;
using Xunit;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Indexing
{
    public class TransactionIndexByMrn : RavenTestBase
    {
        public TransactionIndexByMrn(ITestOutputHelper output) : base(output)
        {
        }

        private class Transaction
        {
            public string MRN { get; set; }
        }

        private class Transaction_ByMrn : AbstractIndexCreationTask<Transaction>
        {
            public Transaction_ByMrn()
            {
                Map = transactions => from transaction in transactions
                                      select new { MRN = transaction.MRN };
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanCreateIndex(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new Transaction_ByMrn().Execute(store);
            }
        }
    }
}
