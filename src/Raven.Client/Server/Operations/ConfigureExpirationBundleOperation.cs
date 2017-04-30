using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Server.Expiration;
using Sparrow.Json;

namespace Raven.Client.Server.Operations
{
    public class ConfigureExpirationOperation : IServerOperation<ConfigureExpirationOperationResult>
    {
        private readonly ExpirationConfiguration _configuration;

        public ConfigureExpirationOperation(ExpirationConfiguration configuration)
        {
            _configuration = configuration;
        }
        public RavenCommand<ConfigureExpirationOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ConfigureExpirationCommand(_configuration, context);
        }
    }

    public class ConfigureExpirationCommand : RavenCommand<ConfigureExpirationOperationResult>
    {
        private readonly ExpirationConfiguration _configuration;
        private readonly JsonOperationContext _context;

        public ConfigureExpirationCommand(ExpirationConfiguration configuration, JsonOperationContext context)
        {
            _configuration = configuration;
            _context = context;
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/expiration/config?name={node.Database}";

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

            Result = JsonDeserializationClient.ConfigureExpirationOperationResult(response);
        }
    }

    public class ConfigureExpirationOperationResult
    {
        public long? ETag { get; set; }
    }
}
