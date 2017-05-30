using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Server.ETL;
using Sparrow.Json;

namespace Raven.Client.Server.Operations.ETL
{
    public class UpdateEtlOperation<T> : IServerOperation<UpdateEtlOperationResult> where T : EtlDestination
    {
        private readonly EtlConfiguration<T> _configuration;
        private readonly string _databaseName;

        public UpdateEtlOperation(EtlConfiguration<T> configuration, string databaseName)
        {
            _configuration = configuration;
            _databaseName = databaseName;
        }

        public RavenCommand<UpdateEtlOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new UpdateEtlCommand(_configuration, _databaseName, context);
        }

        public class UpdateEtlCommand : RavenCommand<UpdateEtlOperationResult>
        {
            private readonly EtlConfiguration<T> _configuration;
            private readonly string _databaseName;
            private readonly JsonOperationContext _context;

            public UpdateEtlCommand(EtlConfiguration<T> configuration, string databaseName, JsonOperationContext context)
            {
                _configuration = configuration;
                _databaseName = databaseName;
                _context = context;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/etl/update?name={_databaseName}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var config = EntityToBlittable.ConvertEntityToBlittable(_configuration, DocumentConventions.Default, _context);
                        _context.Write(stream, config);
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
        public long? ETag { get; set; }
    }
}