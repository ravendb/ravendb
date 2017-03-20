using FastTests;
using Xunit;
using System.Linq;
using Raven.Client.Documents.Indexes;

namespace SlowTests.Bugs.Indexing
{
    public class TransactionIndexByMrn : RavenTestBase
    {
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

        [Fact]
        public void CanCreateIndex()
        {
            using (var store = GetDocumentStore())
            {
                new Transaction_ByMrn().Execute(store);
            }
        }
    }
}
