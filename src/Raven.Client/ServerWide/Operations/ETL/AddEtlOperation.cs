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
    public class AddEtlOperation<T> : IServerOperation<AddEtlOperationResult> where T : ConnectionString
    {
        private readonly EtlConfiguration<T> _configuration;
        private readonly string _databaseName;

        public AddEtlOperation(EtlConfiguration<T> configuration, string databaseName)
        {
            _configuration = configuration;
            _databaseName = databaseName;
        }

        public RavenCommand<AddEtlOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new AddEtlCommand(_configuration, _databaseName, context);
        }

        public class AddEtlCommand : RavenCommand<AddEtlOperationResult>
        {
            private readonly EtlConfiguration<T> _configuration;
            private readonly string _databaseName;
            private readonly JsonOperationContext _context;

            public AddEtlCommand(EtlConfiguration<T> configuration, string databaseName, JsonOperationContext context)
            {
                _configuration = configuration;
                _databaseName = databaseName;
                _context = context;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/etl?name={_databaseName}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
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
