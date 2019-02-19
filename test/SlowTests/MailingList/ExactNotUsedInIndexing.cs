using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class ExactNotUsedInIndexing : RavenTestBase
    {
        public sealed class Document
        {
            public string Id
            {
                get;
                set;
            }

            public string CustomerId
            {
                get;
                set;
            }
        }

        public sealed class Index : AbstractIndexCreationTask<Document>
        {
            public Index()
            {
                Map = orders => from order in orders
                                select new
                                {
                                    order.CustomerId,
                                };

                Index(q => q.CustomerId, FieldIndexing.Exact);
            }
        }


        [Fact]
        public void CanQueryOnExact()
        {
            using (var store = GetDocumentStore())
            {
                new Index().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Document
                    {
                        CustomerId = "customers/1-A"
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Document, Index>()
                                       .Where(q => q.CustomerId == "customers/1-A");

                    var document = query.FirstOrDefault();

                    Assert.NotNull(document);
                }
            }
        }
    }
}
