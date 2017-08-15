using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide.ETL;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.ETL
{
    public class UpdateEtlOperation<T> : IServerOperation<UpdateEtlOperationResult> where T : ConnectionString
    {
        private readonly long _taskId;
        private readonly EtlConfiguration<T> _configuration;
        private readonly string _databaseName;

        public UpdateEtlOperation(long taskId, EtlConfiguration<T> configuration, string databaseName)
        {
            _taskId = taskId;
            _configuration = configuration;
            _databaseName = databaseName;
        }

        public RavenCommand<UpdateEtlOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new UpdateEtlCommand(_taskId, _configuration, _databaseName);
        }

        public class UpdateEtlCommand : RavenCommand<UpdateEtlOperationResult>
        {
            private readonly long _taskId;
            private readonly EtlConfiguration<T> _configuration;
            private readonly string _databaseName;

            public UpdateEtlCommand(long taskId, EtlConfiguration<T> configuration, string databaseName)
            {
                _taskId = taskId;
                _configuration = configuration;
                _databaseName = databaseName;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/etl?id={_taskId}&name={_databaseName}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var config = EntityToBlittable.ConvertEntityToBlittable(_configuration, DocumentConventions.Default, ctx);
                        ctx.Write(stream, config);
                    })
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.UpdateEtlOperationResult(response);
            }
        }
    }

    public class UpdateEtlOperationResult
    {
        public long RaftCommandIndex { get; set; }

        public long TaskId { get; set; }
    }
}