
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class TestRavenIncludes : RavenTestBase
    {
        public TestRavenIncludes(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanIncludeRelatedDocuments(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new SampleData_Index().Execute(store);

                const string name = "John Doe";
                using (var session = store.OpenSession())
                {
                    var sampleData = new SampleData(name);
                    session.Store(sampleData);
                    session.Store(new IncludedData(), sampleData.IncludedIdWithEntityName);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var sampleData = session.Query<SampleData, SampleData_Index>()
                        .Include<SampleData, IncludedData>(x => x.IncludedId)
                        .Customize(customization =>
                        {
                            customization.WaitForNonStaleResults();
                            customization.NoCaching();
                        })
                        .Single(x => x.Name == name);
                    //// This works, but by issuing another query
                    //session.Load<IncludedData>(sampleData.IncludedIdWithPrefix);
                    // This doesn't work, since the document isn't included
                    Assert.True(session.Advanced.IsLoaded(sampleData.IncludedIdWithEntityName), "Included data should be loaded");
                }

                using (var session = store.OpenAsyncSession())
                {
                    var sampleData = await session.Query<SampleData, SampleData_Index>()
                        .Include<SampleData, IncludedData>(x => x.IncludedId)
                        .Customize(customization =>
                        {
                            customization.WaitForNonStaleResults();
                            customization.NoCaching();
                        })
                        .SingleAsync(x => x.Name == name);
                    //// This works, but by issuing another query
                    //session.Load<IncludedData>(sampleData.IncludedIdWithPrefix);
                    // This doesn't work, since the document isn't included
                    Assert.True(session.Advanced.IsLoaded(sampleData.IncludedIdWithEntityName), "Included data should be loaded");
                }

                using (var session = store.OpenAsyncSession())
                {
                    var sampleData = await session.Include<SampleData, IncludedData>(x => x.IncludedId)
                        .LoadAsync("sampleDatas/1-A");

                    Assert.NotNull(sampleData);
                    Assert.True(session.Advanced.IsLoaded(sampleData.IncludedIdWithEntityName), "Included data should be loaded");
                }
            }
        }


        private class SampleData
        {
            public SampleData(string name)
            {
                Name = name;
            }

            public string Id { get; set; }
            public string Name { get; set; }
            public string IncludedId { get { return Id; } }

            /// <summary>
            /// Id of included document with entity name prefix.
            /// </summary>
            public string IncludedIdWithEntityName
            {
                get
                {
                    return string.Format("IncludedDatas/{0}", Id);
                }
            }
        }

        private class SampleData_Index : AbstractIndexCreationTask<SampleData>
        {
            public SampleData_Index()
            {
                Map = docs => from doc in docs select new { doc.Name };
            }
        }

        private class IncludedData
        {
            public string Id { get; set; }
        }
    }

}
