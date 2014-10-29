// -----------------------------------------------------------------------
//  <copyright file="RDBQA_13.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RDBQA_13 : RavenTest
    {

        public static Task<HttpResponseMessage> PatchAsJsonAsync(HttpClient client, string requestUri, string value)
        {
            var content = new StringContent(value);
            var request = new HttpRequestMessage(new HttpMethod("PATCH"), requestUri) { Content = content };

            return client.SendAsync(request);
        }

        [Fact]
        public async Task CanPatchWithNullPrevVal()
        {
            using (var store = (DocumentStore)NewRemoteDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User
                    {
                        Name = "marcin"
                    }, "users/1");
                    s.SaveChanges();
                }

                var client = new HttpClient();
                await PatchAsJsonAsync(client, store.Url.ForDatabase(store.DefaultDatabase) + "/docs/users/1", "[{ Type: 'Set', Name: 'Age', Value: 10, PrevVal: null}]");

                using (var s = store.OpenSession())
                {
                    var user = s.Load<User>("users/1");
                    Assert.Equal(10, user.Age);
                }
            }
        }
    }
}