using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Backups
{
    public class UpdatePeriodicBackupOperation : IMaintenanceOperation<UpdatePeriodicBackupOperationResult>
    {
        private readonly PeriodicBackupConfiguration _configuration;

        public UpdatePeriodicBackupOperation(PeriodicBackupConfiguration configuration)
        {
            _configuration = configuration;
        }

        public RavenCommand<UpdatePeriodicBackupOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new UpdatePeriodicBackupCommand(_configuration);
        }

        private class UpdatePeriodicBackupCommand : RavenCommand<UpdatePeriodicBackupOperationResult>, IRaftCommand
        {
            private readonly PeriodicBackupConfiguration _configuration;

            public UpdatePeriodicBackupCommand(PeriodicBackupConfiguration configuration)
            {
                _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/periodic-backup";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_configuration, ctx)).ConfigureAwait(false))
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ConfigurePeriodicBackupOperationResult(response);
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
