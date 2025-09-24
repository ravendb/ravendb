using System.Net.Http;
using FastTests;
using Raven.Client.Http;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_21088 : RavenTestBase
    {
        public RavenDB_21088(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CheckStorageReportEndpoint()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    var cmd = new GetReportCommand(store.Database, "Documents", true);
                    commands.Execute(cmd);

                    var res = cmd.Result;
                    Assert.NotNull(res);
                    Assert.NotNull(res.Name);
                    Assert.NotNull(res.Type);
                    Assert.NotNull(res.Report);
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CheckAllStorageReportEndpoint()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    var cmd = new GetAllEnvReportCommand(store.Database);
                    commands.Execute(cmd);

                    var res = cmd.Result;
                    Assert.NotNull(res);
                    Assert.NotNull(res.DatabaseName);
                    Assert.NotNull(res.Environments);
                }
            }
        }

        public class StorageReport
        {
            public string Name;
            public string Type;
            public object Report;
        }

        public class AllEnvStorageReport
        {
            public string DatabaseName;
            public StorageReport[] Environments;
        }
        private class GetReportCommand : RavenCommand<StorageReport>
        {
            private readonly string _database;
            private readonly string _type;
            private readonly bool _details;

            public GetReportCommand(string database, string type, bool details)
            {
                _database = database;
                _type = type;
                _details = details;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{_database}/debug/storage/environment/report?name={_database}&type={_type}&details={_details}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get,
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                var deserialize = JsonDeserializationBase.GenerateJsonDeserializationRoutine<StorageReport>();
                Result = deserialize.Invoke(response);
            }

            public override bool IsReadRequest => true;
        }

        private class GetAllEnvReportCommand : RavenCommand<AllEnvStorageReport>
        {
            private readonly string _database;

            public GetAllEnvReportCommand(string database)
            {
                _database = database;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{_database}/debug/storage/all-environments/report";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get,
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                var deserialize = JsonDeserializationBase.GenerateJsonDeserializationRoutine<AllEnvStorageReport>();
                Result = deserialize.Invoke(response);
            }

            public override bool IsReadRequest => true;
        }
    }
}
