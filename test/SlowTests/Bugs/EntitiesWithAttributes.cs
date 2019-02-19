using System.Linq;
using System.Runtime.Serialization;
using FastTests;
using Newtonsoft.Json.Linq;
using Xunit;

namespace SlowTests.Bugs
{
    public class EntitiesWithAttributes : RavenTestBase
    {
        [DataContract]
        private class Item
        {
            [DataMember]
            public string Version { get; set; }
        }

        [Fact]
        public void EntitiesSerializeCorrectlyWithAttributes()
        {
            using (var store = GetDocumentStore())
            {
                var jObject = JObject.FromObject(new Item { Version = "First" }, store.Conventions.CreateSerializer());
                Assert.Equal("First", jObject["Version"]);

//                var rjObject = RavenJObject.FromObject(new Item { Version = "First" }, store.Conventions.CreateSerializer());
//                Assert.Equal("First", rjObject["Version"]);
            }
        }
        [Fact]
        public void PropertiesCanHaveAttributes()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item {Version = "First"});
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // items1 will contain one result
                    var items1 = session.Advanced.DocumentQuery<Item>()
                        .WaitForNonStaleResults()
                        .ToArray();
                    Assert.Equal(1, items1.Length);

                    // items2 will contain zero results, but there should be one result
                    var items2 = session.Query<Item>()
                        .Where(i => i.Version == "First")
                        .ToArray();
                    Assert.Equal(1, items2.Length);

                    // items3 should be same as items1, but there are no results in items3
                    var items3 = session.Advanced.DocumentQuery<Item>()
                        .WaitForNonStaleResults()
                        .ToArray();
                    Assert.Equal(1, items3.Length);
                }
            }
        }
    }
}
