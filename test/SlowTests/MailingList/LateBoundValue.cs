using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class LateBoundValue : RavenTestBase
    {
        private class SampleData : AggregateBaseEx<string>
        {
        }

        private class SampleData_Index : AbstractMultiMapIndexCreationTask<SampleData>
        {
            public SampleData_Index()
            {
                AddMapForAll<AggregateBase>(docs => from doc in docs
                                                    select new
                                                    {
                                                        doc.Name
                                                    });
                StoreAllFields(FieldStorage.Yes);
            }
        }

        private abstract class AggregateBaseEx<TType> : AggregateBase
        {
            public TType AnotherProp { get; set; }
        }

        private abstract class AggregateBase
        {
            public string Id { get; set; }
            public string Name { get; set; }
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
                        Name = "RavenDB",
                        AnotherProp = "AnotherProp"
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Query<SampleData, SampleData_Index>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .FirstOrDefault();

                    Assert.Equal(result.Name, "RavenDB");
                }
            }
        }

        [Fact]
        public void CanLoadDoc()
        {
            using (var store = GetDocumentStore())
            {
                string id;
                using (var session = store.OpenSession())
                {
                    var sampleData = new SampleData
                    {
                        Name = "RavenDB",
                        AnotherProp = "AnotherProp"
                    };
                    session.Store(sampleData);
                    id = sampleData.Id;
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Load<SampleData>(id);

                    Assert.Equal(result.Name, "RavenDB");
                }
            }
        }
    }
}
