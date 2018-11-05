using System.Collections.Generic;
using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12245 : RavenTestBase
    {
        private class TestObj
        {
            public Dictionary<string, Dictionary<string, string>> Dict { get; set; }
        }

        [Fact]
        public void CanUseDictionaryToDictionaryInProjections()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestObj
                    {
                        Dict = new Dictionary<string, Dictionary<string, string>>
                        {
                            {
                                "a", new Dictionary<string, string> { { "b", "c" } }
                            },
                            {
                                "x", new Dictionary<string, string> { { "b", "z" } }
                            }
                        }
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<TestObj>().
                        Select(t => new
                        {
                            res = t.Dict.ToDictionary(x => x.Key, x => x.Value["b"])
                        })
                        .ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(2, result[0].res.Keys.Count);
                    Assert.Equal("c", result[0].res["a"]);
                    Assert.Equal("z", result[0].res["x"]);
                }
            }

        }
    }
}
