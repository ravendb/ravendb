using System;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using SlowTests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11702 : RavenTestBase
    {
        [Fact]
        public async Task CmpgXcngShouldUseClientConventions()
        {
            using (var store = GetDocumentStore(options: new Options
            {
                ModifyDocumentStore = ss => ss.Conventions.CustomizeJsonSerializer = s =>
                {
                    s.ContractResolver = new CamelCasePropertyNamesContractResolver();
                }
            }))
            {
                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var user1 = new User()
                    {
                        Name = "Karmel"
                    };

                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/ayende", user1);
                    await session.SaveChangesAsync();

                    var user = (await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("usernames/ayende"));
                    Assert.Equal(user1.Name, user.Value.Name);
                }

                var user2 = new User()
                {
                    Name = "Karmel2"
                };

                store.Operations.Send(new PutCompareExchangeValueOperation<User>("usersname/viaComamnd", user2, 0));

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var r = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<JObject>("usernames/ayende");
                    Assert.True(r.Value.TryGetValue("name", StringComparison.Ordinal, out JToken name));
                    Assert.Equal("Karmel", name.ToString());

                    r = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<JObject>("usersname/viaComamnd");
                    Assert.True(r.Value.TryGetValue("name", StringComparison.Ordinal, out name));
                    Assert.Equal("Karmel2", name.ToString());
                }
            }

        }
    }
}
