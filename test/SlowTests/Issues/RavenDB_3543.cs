using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3543 : RavenTestBase
    {
        public RavenDB_3543(ITestOutputHelper output) : base(output)
        {
        }

        private class Lead
        {
            public Status Status { get; set; }
        }

        private class Status
        {
            public int Value { get; set; }
        }

        private class Leads_Index : AbstractIndexCreationTask<Lead>
        {

            public Leads_Index()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Status,
                                  Status_Value = doc.Status.Value,
                              };
            }
        }

        [Fact]
        public void SortHints_should_be_recorde_at_most_once_for_each_field()
        {
            using (var store = GetDocumentStore())
            {
                new Leads_Index().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Lead
                    {
                        Status = new Status { Value = 0 }
                    });

                    session.Store(new Lead
                    {
                        Status = new Status { Value = 1 }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<Lead, Leads_Index>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .Where(x => x.Status.Value != 0)
                        .ToList();

                    foreach (var item in result)
                    {
                        Assert.NotEqual(item.Status.Value, 0);
                    }
                }
            }
        }
    }
}
