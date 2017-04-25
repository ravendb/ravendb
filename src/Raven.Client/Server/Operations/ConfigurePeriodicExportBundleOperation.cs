using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Server.expiration;
using Raven.Client.Server.PeriodicExport;
using Sparrow.Json;

namespace Raven.Client.Server.Operations
{
    public class ConfigurePeriodicExportBundleOperation : IServerOperation<ConfigurePeriodicExportBundleOperationResult>
    {
        private PeriodicExportConfiguration _configuration;
        private string _databaseName;

        public ConfigurePeriodicExportBundleOperation(PeriodicExportConfiguration configuration, string databaseName)
        {
            _configuration = configuration;
            _databaseName = databaseName;
        }
        public RavenCommand<ConfigurePeriodicExportBundleOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ConfigurePeriodicExportBundleCommand(_configuration, _databaseName, context);
        }
    }

    public class ConfigurePeriodicExportBundleCommand : RavenCommand<ConfigurePeriodicExportBundleOperationResult>
    {
        private PeriodicExportConfiguration _configuration;
        private readonly string _databaseName;
        private JsonOperationContext _context;

        public ConfigurePeriodicExportBundleCommand(PeriodicExportConfiguration configuration, string databaseName, JsonOperationContext context)
        {
            _configuration = configuration;
            _databaseName = databaseName;
            _context = context;
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/config-periodic-export-bundle?name={_databaseName}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(stream =>
                {
                    var config = EntityToBlittable.ConvertEntityToBlittable(_configuration,DocumentConventions.Default, _context);
                    _context.Write(stream, config);
                })
            };

            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationClient.ConfigurePeriodicExportBundleOperationResult(response);
        }
    }

    public class ConfigurePeriodicExportBundleOperationResult
    {
        public long? ETag { get; set; }
    }
}


