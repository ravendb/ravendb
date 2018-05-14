using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.ETL
{
    public class AddEtlOperation<T> : IMaintenanceOperation<AddEtlOperationResult> where T : ConnectionString
    {
        private readonly EtlConfiguration<T> _configuration;

        public AddEtlOperation(EtlConfiguration<T> configuration)
        {
            _configuration = configuration;
        }

        public RavenCommand<AddEtlOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new AddEtlCommand(conventions, _configuration);
        }

        private class AddEtlCommand : RavenCommand<AddEtlOperationResult>
        {
            private readonly DocumentConventions _conventions;
            private readonly EtlConfiguration<T> _configuration;

            public AddEtlCommand(DocumentConventions conventions, EtlConfiguration<T> configuration)
            {
                _conventions = conventions;
                _configuration = configuration;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/etl";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var config = EntityToBlittable.ConvertCommandToBlittable(_configuration, ctx);
                        ctx.Write(stream, config);
                    })
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.AddEtlOperationResult(response);
            }
        }
    }

    public class AddEtlOperationResult
    {
        public long RaftCommandIndex { get; set; }

        public long TaskId { get; set; }
    }
}
