using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class RestoreBackupOperation : IServerOperation<OperationIdResult>
    {
        private readonly RestoreBackupConfiguration _restoreConfiguration;
        private readonly RestoreFromS3Configuration _restoreFromS3Configuration;
        private readonly RestoreType _restoreType;

        public string NodeTag;

        public RestoreBackupOperation(RestoreBackupConfiguration restoreConfiguration, string nodeTag = null)
        {
            _restoreConfiguration = restoreConfiguration;
            NodeTag = nodeTag;
            _restoreType = RestoreType.Local;
        }

        public RestoreBackupOperation(RestoreFromS3Configuration restoreConfiguration, string nodeTag = null)
        {
            _restoreFromS3Configuration = restoreConfiguration;
            NodeTag = nodeTag;
            _restoreType = RestoreType.S3;
        }

        public RavenCommand<OperationIdResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            switch (_restoreType)
            {
                case RestoreType.Local:
                    return new RestoreBackupCommand(_restoreConfiguration, _restoreType, NodeTag);
                case RestoreType.S3:
                    return new RestoreBackupCommand(_restoreFromS3Configuration, _restoreType, NodeTag);
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        internal enum RestoreType
        {
            Local,
            S3
        }

        private class RestoreBackupCommand : RavenCommand<OperationIdResult>
        {
            public override bool IsReadRequest => false;
            private readonly RestoreBackupConfigurationBase _restoreConfiguration;
            private readonly RestoreType _restoreType;

            public RestoreBackupCommand(RestoreBackupConfigurationBase restoreConfiguration, RestoreType restoreType, string nodeTag = null)
            {
                _restoreConfiguration = restoreConfiguration;
                _restoreType = restoreType;
                SelectedNodeTag = nodeTag;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/restore/database?type={_restoreType.ToString()}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var config = EntityToBlittable.ConvertCommandToBlittable(_restoreConfiguration, ctx);
                        ctx.Write(stream, config);
                    })
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.OperationIdResult(response);
            }
        }
    }
}
