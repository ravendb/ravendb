using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Server.expiration;
using Sparrow.Json;

namespace Raven.Client.Server.Operations
{
    public class ConfigureExpirationBundleOperation : IServerOperation<ConfigureExpirationBundleOperationResult>
    {
        private ExpirationConfiguration _configuration;
        private string _databaseName;

        public ConfigureExpirationBundleOperation(ExpirationConfiguration configuration, string databaseName)
        {
            _configuration = configuration;
            _databaseName = databaseName;
        }
        public RavenCommand<ConfigureExpirationBundleOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ConfigureExpirationBundleCommand(_configuration, _databaseName, context);
        }
    }

    public class ConfigureExpirationBundleCommand : RavenCommand<ConfigureExpirationBundleOperationResult>
    {
        private ExpirationConfiguration _configuration;
        private readonly string _databaseName;
        private JsonOperationContext _context;

        public ConfigureExpirationBundleCommand(ExpirationConfiguration configuration, string databaseName, JsonOperationContext context)
        {
            _configuration = configuration;
            _databaseName = databaseName;
            _context = context;
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/config-expiration-bundle?name={_databaseName}";

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

            Result = JsonDeserializationClient.ConfigureExpirationBundleOperationResult(response);
        }
    }

    public class ConfigureExpirationBundleOperationResult
    {
        public long? ETag { get; set; }
    }
}
