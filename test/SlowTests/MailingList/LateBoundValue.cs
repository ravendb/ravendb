using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class LateBoundValue : RavenTestBase
    {
        public LateBoundValue(ITestOutputHelper output) : base(output)
        {
        }

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

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanIndexAndQuery(Options options)
        {
            using (var store = GetDocumentStore(options))
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

                Indexes.WaitForIndexing(store);
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

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanLoadDoc(Options options)
        {
            using (var store = GetDocumentStore(options))
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
