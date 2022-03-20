using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class ConcurrencyTests : RavenTestBase
    {
        public ConcurrencyTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanSaveReferencingAndReferencedDocumentsInOneGo()
        {
            using (var store = GetDocumentStore())
            {
                new Sections().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new SectionData { Id = "sections/1", Referencing = null });
                    session.Store(new SectionData { Id = "sections/2", Referencing = "sections/1" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    session.Advanced.UseOptimisticConcurrency = true;
                    var foos = session.Query<SectionData>().ToList();

                    foreach (var sectionData in foos)
                    {
                        sectionData.Count++;
                    }

                    session.SaveChanges();
                }
            }
        }

        private class SectionData
        {
            public string Id { get; set; }
            public string Referencing { get; set; }
            public int Count { get; set; }
        }

        private class Sections : AbstractIndexCreationTask<SectionData>
        {
            public Sections()
            {
                Map = datas =>
                      from data in datas
                      select new
                      {
                          _ = LoadDocument<SectionData>(data.Referencing)
                      };
            }
        }
    }
}
