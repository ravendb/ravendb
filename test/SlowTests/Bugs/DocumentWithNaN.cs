// -----------------------------------------------------------------------
//  <copyright file="DocumentWithNaN.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class Number
    {
        public float FNumber = Single.NaN;
    }
    public class DocumentWithNaN : RavenTestBase
    {
        public DocumentWithNaN(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanSaveUsingLegacyMode()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenAsyncSession())
            {
                var httpClient = store.GetRequestExecutor().HttpClient;
                var httpResponseMessage = await httpClient.SendAsync(new HttpRequestMessage(HttpMethod.Put, $"{store.Urls.First()}/databases/{store.Database}/docs?id=items/1-A")
                {
                    Content = new StringContent("{'item': NaN}")
                }.WithConventions(store.Conventions));

                Assert.True(httpResponseMessage.IsSuccessStatusCode);

                await session.StoreAsync(new Number());
                await session.SaveChangesAsync();
                var num = await session.LoadAsync<Number>("Numbers/1-A");
                Assert.Equal(float.NaN, num.FNumber);
            }
        }

    }
}
