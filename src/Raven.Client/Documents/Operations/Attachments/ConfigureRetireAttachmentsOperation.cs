using System;
using System.Net.Http;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Attachments
{
    public sealed class ConfigureRetireAttachmentsOperation : IMaintenanceOperation<ConfigureRetireAttachmentsOperationResult>
    {
        private readonly RetireAttachmentsConfiguration _configuration;

        public ConfigureRetireAttachmentsOperation(RetireAttachmentsConfiguration configuration)
        {   
            if(configuration.RetirePeriods == null)
                throw new ArgumentNullException(nameof(configuration.RetirePeriods));
            if (configuration.RetirePeriods.Count == 0)
                throw new ArgumentException("RetirePeriods must contain at least one period", nameof(configuration.RetirePeriods));

            if (BackupConfiguration.CanBackupUsing(configuration.S3Settings) == false &&
                BackupConfiguration.CanBackupUsing(configuration.AzureSettings) == false &&
                BackupConfiguration.CanBackupUsing(configuration.GoogleCloudSettings) == false &&
                BackupConfiguration.CanBackupUsing(configuration.FtpSettings) == false &&
                BackupConfiguration.CanBackupUsing(configuration.GlacierSettings)
               )
            {
                throw new ArgumentException("At least one destination must be configured", nameof(configuration));
            }

            _configuration = configuration;
        }

        public RavenCommand<ConfigureRetireAttachmentsOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new ConfigureAttachmentsRetireCommand(conventions, _configuration);
        }

        private sealed class ConfigureAttachmentsRetireCommand : RavenCommand<ConfigureRetireAttachmentsOperationResult>, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly RetireAttachmentsConfiguration _configuration;

            public ConfigureAttachmentsRetireCommand(DocumentConventions conventions, RetireAttachmentsConfiguration configuration)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/attachments/retire/config";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_configuration, ctx)).ConfigureAwait(false), _conventions)
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ConfigureRetireAttachmentsOperationResult(response);
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
