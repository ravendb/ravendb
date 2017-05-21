using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Server.PeriodicExport;
using Sparrow.Json;

namespace Raven.Client.Server.Operations
{
    public class ConfigurePeriodicBackupOperation : IServerOperation<ConfigurePeriodicBackupOperationResult>
    {
        private readonly PeriodicBackupConfiguration _configuration;
        private readonly string _databaseName;

        public ConfigurePeriodicBackupOperation(PeriodicBackupConfiguration configuration, string databaseName)
        {
            _configuration = configuration;
            _databaseName = databaseName;
        }
        public RavenCommand<ConfigurePeriodicBackupOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ConfigurePeriodicBackupCommand(_configuration, _databaseName, context);
        }
    }

    public class ConfigurePeriodicBackupCommand : RavenCommand<ConfigurePeriodicBackupOperationResult>
    {
        private readonly PeriodicBackupConfiguration _configuration;
        private readonly string _databaseName;
        private readonly JsonOperationContext _context;

        public ConfigurePeriodicBackupCommand(PeriodicBackupConfiguration configuration, string databaseName, JsonOperationContext context)
        {
            _configuration = configuration;
            _databaseName = databaseName;
            _context = context;
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/periodic-backup/config?name={_databaseName}";

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

    public class ConfigurePeriodicBackupOperationResult
    {
        public long? ETag { get; set; }
    }
}


