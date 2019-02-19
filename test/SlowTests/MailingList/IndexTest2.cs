using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class IndexTest2 : RavenTestBase
    {
        private class SampleData
        {
            public string Name { get; set; }
        }

        private class SampleData_Index : AbstractIndexCreationTask<SampleData>
        {
            public SampleData_Index()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Name
                              };
            }
        }

        [Fact]
        public void CanIndexAndQuery()
        {
            using (var store = GetDocumentStore())
            {
                new SampleData_Index().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new SampleData
                    {
                        Name = "RavenDB"
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<SampleData, SampleData_Index>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .FirstOrDefault();

                    var test = session.Query<SampleData, SampleData_Index>().OrderBy(x => x.Name).ToList();
                    Assert.Equal(test[0].Name, "RavenDB");
                }
            }
        }
    }

}
