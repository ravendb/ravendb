using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Operations;
using Sparrow.Json;
using Xunit;

namespace FastTests.Server.Documents
{
    public class Collections : RavenNewTestBase
    {
        [Fact]
        public void CanSurviveRestart()
        {
            var path = NewDataPath();
            using (var store = GetDocumentStore(path: path))
            {
                using (var commands = store.Commands())
                {
                    commands.Put("orders/1", null, new { }, new Dictionary<string, string>
                    {
                        {Constants.Headers.RavenEntityName, "Orders"}
                    });

                    commands.Put("orders/2", null, new { }, new Dictionary<string, string>
                    {
                        {Constants.Headers.RavenEntityName, "orders"}
                    });

                    commands.Put("people/1", null, new { }, new Dictionary<string, string>
                    {
                        {Constants.Headers.RavenEntityName, "People"}
                    });

                    var collectionStats = store.Admin.Send(new GetCollectionStatisticsOperation());

                    Assert.Equal(2, collectionStats.Collections.Count);

                    var orders = collectionStats.Collections.First(x => x.Key == "Orders");
                    Assert.Equal(2, orders.Value);

                    var people = collectionStats.Collections.First(x => x.Key == "People");
                    Assert.Equal(1, people.Value);
                }
            }

            using (var store = GetDocumentStore(path: path))
            {
                var collectionStats = store.Admin.Send(new GetCollectionStatisticsOperation());

                Assert.Equal(2, collectionStats.Collections.Count);

                var orders = collectionStats.Collections.First(x => x.Key == "Orders");
                Assert.Equal(2, orders.Value);

                var people = collectionStats.Collections.First(x => x.Key == "People");
                Assert.Equal(1, people.Value);
            }
        }

        private class GetCollectionStatisticsOperation : IAdminOperation<GetCollectionStatisticsOperation.Result>
        {
            public class Result
            {
                public int NumberOfDocuments { get; set; }

                public Dictionary<string, long> Collections { get; set; }
            }

            public RavenCommand<Result> GetCommand(DocumentConvention conventions, JsonOperationContext context)
            {
                return new GetCollectionStatisticsCommand(conventions);
            }

            private class GetCollectionStatisticsCommand : RavenCommand<Result>
            {
                private readonly DocumentConvention _conventions;

                public GetCollectionStatisticsCommand(DocumentConvention conventions)
                {
                    if (conventions == null)
                        throw new ArgumentNullException(nameof(conventions));

                    _conventions = conventions;
                }

                public override bool IsReadRequest => true;

                public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
                {
                    url = $"{node.Url}/databases/{node.Database}/collections/stats";
                    return new HttpRequestMessage
                    {
                        Method = HttpMethod.Get
                    };
                }

                public override void SetResponse(BlittableJsonReaderObject response)
                {
                    if (response == null)
                        ThrowInvalidResponse();

                    Result = (Result)_conventions.DeserializeEntityFromBlittable(typeof(Result), response);
                }
            }
        }
    }
}