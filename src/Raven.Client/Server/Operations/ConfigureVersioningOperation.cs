using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Server.Expiration;
using Raven.Client.Server.Versioning;
using Sparrow.Json;

namespace Raven.Client.Server.Operations
{
    public class ConfigureVersioningOperation : IServerOperation<ConfigureVersioningOperationResult>
    {
        private readonly VersioningConfiguration _configuration;
        private string _databaseName;

        public ConfigureVersioningOperation(VersioningConfiguration configuration, string databaseName)
        {
            _configuration = configuration;
            _databaseName = databaseName;
        }
        public RavenCommand<ConfigureVersioningOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ConfigureVersioningCommand(_configuration, _databaseName, context);
        }
    }

    public class ConfigureVersioningCommand : RavenCommand<ConfigureVersioningOperationResult>
    {
        private readonly VersioningConfiguration _configuration;
        private readonly string _databaseName;
        private readonly JsonOperationContext _context;

        public ConfigureVersioningCommand(VersioningConfiguration configuration, string databaseName, JsonOperationContext context)
        {
            _configuration = configuration;
            _databaseName = databaseName;
            _context = context;
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/versioning/config?name={_databaseName}";

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

            Result = JsonDeserializationClient.ConfigureVersioningOperationResult(response);
        }
    }

    public class ConfigureVersioningOperationResult
    {
        public long? ETag { get; set; }
    }
}
