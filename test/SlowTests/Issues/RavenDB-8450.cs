using System.IO;
using System.Net.Http;
using System.Text;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8450 : RavenTestBase
    {
        [Theory]
        [InlineData("hello\nthere", "\n")]
        [InlineData("hello\r\nthere", "\r\n")]
        public void CanGetSubscriptionsResultsWithEscapeHandling(string input, string shouldNotContain)
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new PersonWithAddress
                    {
                        Address = new Address
                        {
                            Country = input
                        }
                    });
                    s.SaveChanges();
                }

                var result = store.Operations.Send(new SubscriptionTryoutOperation(new SubscriptionTryout
                {
                    Query = "from PersonWithAddresses as u select { Self: u }"
                }));

                Assert.DoesNotContain(shouldNotContain, result);
            }
        }

        public class SubscriptionTryoutOperation : RavenCommand<string>, IOperation<string>
        {
            private readonly SubscriptionTryout _tryout;

            public SubscriptionTryoutOperation(SubscriptionTryout tryout)
            {
                _tryout = tryout;
                ResponseType = RavenCommandResponseType.Raw;
            }

            public RavenCommand<string> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
            {
                return this;
            }

            public override bool IsReadRequest { get; } = false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        using (var writer = new BlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteStartObject();
                            writer.WritePropertyName(nameof(SubscriptionTryout.ChangeVector));
                            writer.WriteString(_tryout.ChangeVector);
                            writer.WritePropertyName(nameof(SubscriptionTryout.Query));
                            writer.WriteString(_tryout.Query);
                            writer.WriteEndObject();
                        }
                    })
                };

                var sb = new StringBuilder($"{node.Url}/databases/{node.Database}/subscriptions/try?pageSize=10");

                url = sb.ToString();

                return request;
            }

            public override void SetResponseRaw(HttpResponseMessage response, Stream stream, JsonOperationContext context)
            {
                Result = new StreamReader(stream).ReadToEnd();
            }
        }
    }
}
