using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
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
            return new AddEtlCommand(_configuration);
        }

        private class AddEtlCommand : RavenCommand<AddEtlOperationResult>, IRaftCommand
        {
            private readonly EtlConfiguration<T> _configuration;

            public AddEtlCommand(EtlConfiguration<T> configuration)
            {
                _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
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
                        var config = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_configuration, ctx);
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

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }

    public class AddEtlOperationResult
    {
        public long RaftCommandIndex { get; set; }

        public long TaskId { get; set; }
    }
}
