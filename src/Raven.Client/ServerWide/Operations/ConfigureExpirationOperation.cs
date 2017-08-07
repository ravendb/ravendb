using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide.Expiration;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class ConfigureExpirationOperation : IServerOperation<ConfigureExpirationOperationResult>
    {
        private readonly ExpirationConfiguration _configuration;
        private readonly string _databaseName;

        public ConfigureExpirationOperation(ExpirationConfiguration configuration, string databaseName)
        {
            _configuration = configuration;
            _databaseName = databaseName;
        }

        public RavenCommand<ConfigureExpirationOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ConfigureExpirationCommand(_configuration, _databaseName, context);
        }

        public class ConfigureExpirationCommand : RavenCommand<ConfigureExpirationOperationResult>
        {
            private readonly ExpirationConfiguration _configuration;
            private readonly JsonOperationContext _context;
            private readonly string _databaseName;

            public ConfigureExpirationCommand(ExpirationConfiguration configuration, string databaseName, JsonOperationContext context)
            {
                _configuration = configuration;
                _context = context;
                _databaseName = databaseName;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/expiration/config?name={_databaseName}";

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
    }
}