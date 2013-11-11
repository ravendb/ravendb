using System.Linq;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class TestRavenIncludes : RavenTestBase
    {
        [Fact]
        public void CanIncludeRelatedDocuments()
        {
            using (var store = NewDocumentStore())
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
                                            .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                                            .Single(x => x.Name == name);
                    //// This works, but by issuing another query
                    //session.Load<IncludedData>(sampleData.IncludedIdWithPrefix);
                    // This doesn't work, since the document isn't included
                    Assert.True(session.Advanced.IsLoaded(sampleData.IncludedIdWithEntityName), "Included data should be loaded");
                }
            }
        }


        class SampleData
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

        class SampleData_Index : AbstractIndexCreationTask<SampleData>
        {
            public SampleData_Index()
            {
                Map = docs => from doc in docs select new { doc.Name };
            }
        }

        class IncludedData
        {
            public string Id { get; set; }
        }
    }

}