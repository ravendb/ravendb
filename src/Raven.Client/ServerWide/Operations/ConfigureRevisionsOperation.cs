using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide.Revisions;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class ConfigureRevisionsOperation : IServerOperation<ConfigureRevisionsOperationResult>
    {
        private readonly RevisionsConfiguration _configuration;
        private readonly string _databaseName;

        public ConfigureRevisionsOperation(RevisionsConfiguration configuration, string databaseName)
        {
            _configuration = configuration;
            _databaseName = databaseName;
        }
        public RavenCommand<ConfigureRevisionsOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ConfigureRevisionsCommand(_configuration, _databaseName, context);
        }
    }

    public class ConfigureRevisionsCommand : RavenCommand<ConfigureRevisionsOperationResult>
    {
        private readonly RevisionsConfiguration _configuration;
        private readonly string _databaseName;
        private readonly JsonOperationContext _context;

        public ConfigureRevisionsCommand(RevisionsConfiguration configuration, string databaseName, JsonOperationContext context)
        {
            _configuration = configuration;
            _databaseName = databaseName;
            _context = context;
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/revisions/config?name={_databaseName}";

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

            Result = JsonDeserializationClient.ConfigureRevisionsOperationResult(response);
        }
    }

    public class ConfigureRevisionsOperationResult
    {
        public long? ETag { get; set; }
    }
}
