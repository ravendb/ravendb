using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Xunit;

namespace Raven.Tests.Bugs
{
    public class OrderOfInsertionDoesNotAffectQuerying : LocalClientTest
    {
        [Fact]
        public void Works()
        {
            using(var store = NewDocumentStore())
            {
                store.DatabaseCommands.Put("users/1", null,
                                           new JObject
                                           {
                                               {"Name", "user1"},
                                               {"Tags", new JArray(new[]{"abc", "def"})}
                                           },
                                           new JObject());

                store.DatabaseCommands.Put("users/2", null,
                                           new JObject
                                           {
                                               {"Name", "user2"},
                                           },
                                           new JObject());

                var queryResult = store.DatabaseCommands.Query("dynamic", new IndexQuery
                {
                    Query = "Tags,:abc"
                }, new string[0]);

                Assert.Equal(1, queryResult.Results.Count);
            }
        }

        [Fact]
        public void DoesNotWorks()
        {
            using (var store = NewDocumentStore())
            {
                store.DatabaseCommands.Put("users/1", null,
                                           new JObject
                                           {
                                               {"Name", "user1"},
                                           },
                                           new JObject());

                store.DatabaseCommands.Put("users/2", null,
                                           new JObject
                                           {
                                               {"Name", "user2"},
                                               {"Tags", new JArray(new[]{"abc", "def"})}
                                           },
                                           new JObject());

                var queryResult = store.DatabaseCommands.Query("dynamic", new IndexQuery
                {
                    Query = "Tags,:abc"
                }, new string[0]);

                Assert.Equal(1, queryResult.Results.Count);
            }
        }
    }
}