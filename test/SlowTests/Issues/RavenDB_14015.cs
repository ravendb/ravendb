using System.Collections.Generic;
using System.Linq;
using FastTests;
using Newtonsoft.Json.Linq;
using Xunit;

namespace SlowTests.Issues
{
    public class X
    {
        public string Id { get; set; }
        public JObject J { get; set; }
        public List<KeyValuePair<string,string>> A { get; set; }
    }

    public class UserSelection
    {
        public string UserId { get; set; }
    }

    public class RavenDB_14015 : RavenTestBase
    {
        [Fact]
        public void ShouldNotSimplifyJObjectProperty()
        {
            var jsonString = 
                @"{
  ""CrazyField"": {
    ""$type"": ""SlowTests.Issues.UserSelection[], SlowTests"",
    ""$values"": [
      {
        ""UserId"": ""144a29c3-8cb3-4f10-b256-f0d253fbe255""
      },
      {
        ""UserId"": ""944f14c7-e057-41ee-b2f7-a457bca4e1c4""
      },
      {
        ""UserId"": ""18287ffd-e6fd-4236-a1e7-4805498bbf4c""
      }
    ]
  }
}";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new X
                    {
                        J = JObject.Parse(jsonString),
                        A = new List<KeyValuePair<string, string>> { new KeyValuePair<string, string>("beer", "🍺"),
                            new KeyValuePair<string, string>("moreBeer", "🍻") }
                    }, "blah");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<X>("blah");
                    Assert.NotNull(doc.J["CrazyField"]["$type"]);
                    Assert.NotNull(doc.J["CrazyField"]["$values"]);

                    var kvp = doc.A.First();
                    Assert.Equal("beer", kvp.Key);
                    Assert.Equal("🍺", kvp.Value);

                    kvp = doc.A.Last();
                    Assert.Equal("moreBeer", kvp.Key);
                    Assert.Equal("🍻", kvp.Value);
                }
            }
        }
    }
}
