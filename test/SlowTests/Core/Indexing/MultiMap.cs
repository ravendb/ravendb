using System.Linq;
using Xunit.Abstractions;

using FastTests;

using SlowTests.Core.Utils.Indexes;

using Xunit;

using Company = SlowTests.Core.Utils.Entities.Company;
using Headquater = SlowTests.Core.Utils.Entities.Headquater;
using ISearchable = SlowTests.Core.Utils.Entities.ISearchable;

namespace SlowTests.Core.Indexing
{
    public class MultiMap : RavenTestBase
    {
        public MultiMap(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanCreateAndSearchMultiMapIndex()
        {
            using (var store = GetDocumentStore())
            {
                var index = new MultiMapIndex();
                index.Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Address1 = "token1", Address2 = "some address 2 token2", Address3 = "some address 3 token3" });
                    session.Store(new Company { Address1 = "some address" });
                    session.Store(new Headquater { Name = "token1" });
                    session.Store(new Headquater { Name = "name", Address1 = "some addr token1" });
                    session.SaveChanges();
                    Indexes.WaitForIndexing(store);

                    var results = session.Advanced
                        .DocumentQuery<ISearchable>(index.IndexName)
                        .Search("Content", "token1")
                        .ToArray();

                    Assert.Equal(2, results.Length);
                    Assert.Equal("token1", results[0].Address1);
                    Assert.Equal("some addr token1", results[1].Address1);

                    results = session.Advanced
                        .DocumentQuery<ISearchable>(index.IndexName)
                        .Search("Content", "token2")
                        .ToArray();

                    Assert.Equal(1, results.Length);
                    Assert.Equal("some address 2 token2", results[0].Address2);
                }
            }
        }
    }
}
