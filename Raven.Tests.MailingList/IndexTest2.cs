using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
    public class IndexTest2 : RavenTestBase
    {

        public class SampleData
        {
            public string Name { get; set; }
        }

        public class SampleData_Index : AbstractIndexCreationTask<SampleData>
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
            using (var store = NewDocumentStore())
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
                        .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                        .FirstOrDefault();

                    var test = session.Query<SampleData, SampleData_Index>().OrderBy(x => x.Name).ToList();
                    Assert.Equal(test[0].Name, "RavenDB");
                }
            }
        }
    }

}
