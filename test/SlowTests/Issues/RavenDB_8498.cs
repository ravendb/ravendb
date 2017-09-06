using System;
using System.Net.Http;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Database;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8498 : RavenTestBase
    {
        [Fact]
        public void SholdBeAbleToDeleteADatabaseViaRequestBody()
        {
            using (var store = GetDocumentStore())
            {
                var dbName1 = $"{store.Database}_1";
                var dbName2 = $"{store.Database}_2";

                store.Admin.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(dbName1)));
                store.Admin.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(dbName2)));

                Assert.Equal(0, store.Admin.ForDatabase(dbName1).Send(new GetStatisticsOperation()).CountOfDocuments);
                Assert.Equal(0, store.Admin.ForDatabase(dbName2).Send(new GetStatisticsOperation()).CountOfDocuments);

                using (var commands = store.Commands())
                {
                    var command = new DeleteDatabasesCommand(new[] { dbName1, dbName2 }, 30);

                    commands.RequestExecutor.Execute(command, commands.Context);
                }

                try
                {
                    store.Admin.ForDatabase(dbName1).Send(new GetStatisticsOperation());
                }
                catch (DatabaseDoesNotExistException)
                {
                }
                catch (DatabaseDisabledException)
                {
                }

                try
                {
                    store.Admin.ForDatabase(dbName2).Send(new GetStatisticsOperation());
                }
                catch (DatabaseDoesNotExistException)
                {
                }
                catch (DatabaseDisabledException)
                {
                }
            }
        }

        private class DeleteDatabasesCommand : RavenCommand<DeleteDatabaseResult>
        {
            private readonly string[] _names;
            private readonly int _timeInSec;

            public DeleteDatabasesCommand(string[] names, int timeInSec)
            {
                _names = names;
                _timeInSec = timeInSec;
                ResponseType = RavenCommandResponseType.Object;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/databases";

                if (_timeInSec > 0)
                {
                    url += $"?confirmationTimeoutInSec={_timeInSec}";
                }

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Delete,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var databases = new DynamicJsonArray();
                        foreach (var name in _names)
                            databases.Add(name);

                        var djv = new DynamicJsonValue
                        {
                            ["Databases"] = databases
                        };

                        var json = ctx.ReadObject(djv, "databases");

                        ctx.Write(stream, json);
                    })
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationClient.DeleteDatabaseResult(response);
            }

            public override bool IsReadRequest => false;
        }
    }
}
