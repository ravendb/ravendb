using FastTests;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class Vandieren : RavenTestBase
    {

        public class DemoIndex : AbstractIndexCreationTask<DemoObject>
        {
            public class Result
            {
                public string Id { get; set; }
                public string Hash { get; set; }
            }

            public DemoIndex()
            {
                Map = files =>
                    from file in files
                    select new { file.Id, file.Hash };
            }
        }

        public class DemoObject
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Hash { get; set; }
        }

        public class DemoContainer
        {
            public string Id { get; set; }
        }

        [Fact]
        public void WillNotGetInvalidCastException()
        {
            using (var store = GetDocumentStore())
            {
                new DemoIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new DemoObject { Id = "TEST 1", Hash = "HASH 1", Name = "1" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var indexResults = session.Query<DemoIndex.Result, DemoIndex>()
                        .Customize(x => x.WaitForNonStaleResults())
                                              .Where(r => r.Hash == "HASH 1")
                                              .Take(10)
                                              .OfType<DemoObject>()
                                              .ToArray()
                                              .Select(x => new DemoContainer { Id = x.Id })
                                              .ToArray();

                    var ids = indexResults.Select(x => x.Id).ToArray();
                    var loadResults = session.Load<DemoObject>(ids);
                }
            }
        }
    }
}
