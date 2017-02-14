using FastTests;
using Newtonsoft.Json;
using System.Linq;
using Newtonsoft.Json.Linq;
using Xunit;

namespace SlowTests.Bugs.Queries
{
    public class Fetching : RavenTestBase
    {
        [Fact(Skip = "RavenDB-6155")]
        public void CanFetchMultiplePropertiesFromCollection()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    for (int i = 0; i < 3; i++)
                    {
                        s.Store(new
                        {
                            Id = "item-"+i,
                            Tags = new[]
                            {

                                new {Id = i%2, Id3 = i%3},
                                new {Id = i%2 + 1, Id3 = i%3 + 2}
                            }
                        });
                    }
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var objects = s.Advanced.DocumentQuery<dynamic>()
                        .WaitForNonStaleResults()
                        .SelectFields<JObject>("Tags,Id", "Tags,Id3")
                        .OrderBy("Id")
                        .ToArray();

                    Assert.Equal(3, objects.Length);

                    var expected = new[]
                                    {
                                        "\"Tags\":[{\"Id\":0,\"Id3\":0},{\"Id\":1,\"Id3\":2}]",
                                        "\"Tags\":[{\"Id\":1,\"Id3\":1},{\"Id\":2,\"Id3\":3}]",
                                        "\"Tags\":[{\"Id\":0,\"Id3\":2},{\"Id\":1,\"Id3\":4}]",
                                    };

                    for (int i = 0; i < 3; i++)
                    {
                        Assert.Contains(expected[i], objects[i].ToString(Formatting.None));
                    }
                }
            }
        }
        
    }
}
