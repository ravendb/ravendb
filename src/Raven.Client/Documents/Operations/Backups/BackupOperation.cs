using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Backups
{
    public sealed class BackupOperation : IMaintenanceOperation<OperationIdResult<StartBackupOperationResult>>
    {
        private readonly BackupConfiguration _backupConfiguration;
        
        public BackupOperation(BackupConfiguration backupConfiguration)
        {
            _backupConfiguration = backupConfiguration ?? throw new ArgumentNullException(nameof(backupConfiguration));

            if (_backupConfiguration.HasBackup() == false)
                throw new InvalidOperationException("Cannot start the one-time backup using the provided configuration since the backup configuration defines no destinations.");
        }

        public RavenCommand<OperationIdResult<StartBackupOperationResult>> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new BackupCommand(conventions, _backupConfiguration, null);
        }

        internal sealed class BackupCommand : RavenCommand<OperationIdResult<StartBackupOperationResult>>
        {
            private readonly DateTime? _startTime;
            public override bool IsReadRequest => false;
            private readonly DocumentConventions _conventions;
            private readonly BackupConfiguration _backupConfiguration;
            private readonly long? _operationId;

            public BackupCommand(DocumentConventions conventions, BackupConfiguration backupConfiguration, long? operationId = null)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _backupConfiguration = backupConfiguration;
                _operationId = operationId;
            }

            internal BackupCommand(DocumentConventions conventions, BackupConfiguration backupConfiguration, DateTime startTime, long? operationId = null) : 
                this(conventions, backupConfiguration, operationId)
            {
                _startTime = startTime;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/backup?";

                if (_operationId.HasValue)
                    url += $"&operationId={_operationId}";

                if (_startTime.HasValue)
                    url += $"&startTime={_startTime.Value.GetDefaultRavenFormat()}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        var config = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_backupConfiguration, ctx);
                        await ctx.WriteAsync(stream, config).ConfigureAwait(false);
                    }, _conventions)
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                var result = JsonDeserializationClient.BackupDatabaseNowResult(response);
                var operationIdResult = JsonDeserializationClient.OperationIdResult(response);

                // OperationNodeTag used to fetch operation status
                operationIdResult.OperationNodeTag ??= result.ResponsibleNode;
                Result = operationIdResult.ForResult(result);
            }
        }
    }
}
