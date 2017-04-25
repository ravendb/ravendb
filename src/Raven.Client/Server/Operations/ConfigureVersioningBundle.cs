using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Server.expiration;
using Raven.Server.Documents.Versioning;
using Sparrow.Json;

namespace Raven.Client.Server.Operations
{
    public class ConfigureVersioningBundle : IServerOperation<ConfigureVersioningBundleOperationResult>
    {
        private VersioningConfiguration _configuration;
        private string _databaseName;

        public ConfigureVersioningBundle(VersioningConfiguration configuration, string databaseName)
        {
            _configuration = configuration;
            _databaseName = databaseName;
        }
        public RavenCommand<ConfigureVersioningBundleOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ConfigureVersioningBundleCommand(_configuration, _databaseName, context);
        }
    }

    public class ConfigureVersioningBundleCommand : RavenCommand<ConfigureVersioningBundleOperationResult>
    {
        private VersioningConfiguration _configuration;
        private readonly string _databaseName;
        private JsonOperationContext _context;

        public ConfigureVersioningBundleCommand(VersioningConfiguration configuration, string databaseName, JsonOperationContext context)
        {
            _configuration = configuration;
            _databaseName = databaseName;
            _context = context;
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/config-versioning-bundle?name={_databaseName}";

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

            Result = JsonDeserializationClient.ConfigureVersioningBundleOperationResult(response);
        }
    }

    public class ConfigureVersioningBundleOperationResult
    {
        public long? ETag { get; set; }
    }
}
