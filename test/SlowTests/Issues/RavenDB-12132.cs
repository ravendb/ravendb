using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12132 : RavenTestBase
    {
        [Fact]
        public async Task CanPutObjectWithId()
        {
            DoNotReuseServer();
            var store = GetDocumentStore();
            var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("test", new User
            {
                Id = "users/1",
                Name = "Grisha"
            }, 0));

            Assert.True(res.Successful);
            Assert.Equal("Grisha", res.Value.Name);
            Assert.Equal("users/1", res.Value.Id);
        }
    }
}
