using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Raven.Server.Documents.PeriodicBackup.GoogleCloud;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.Studio.Processors
{
    internal class StudioTasksHandlerProcessorForTestPeriodicBackupCredentials<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        public StudioTasksHandlerProcessorForTestPeriodicBackupCredentials([NotNull] TRequestHandler requestHandler,
            [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
            : base(requestHandler, contextPool)
        {
        }

        public override async ValueTask ExecuteAsync()
        {
            var type = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");

            if (Enum.TryParse(type, out PeriodicBackupConnectionType connectionType) == false)
                throw new ArgumentException($"Unknown backup connection: {type}");

            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                DynamicJsonValue result;
                try
                {
                    var connectionInfo = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "test-connection");
                    switch (connectionType)
                    {
                        case PeriodicBackupConnectionType.S3:
                            var s3Settings = JsonDeserializationClient.S3Settings(connectionInfo);
                            using (var awsClient = new RavenAwsS3Client(s3Settings, RequestHandler.ServerStore.Configuration.Backup, cancellationToken: RequestHandler.ServerStore.ServerShutdown))
                            {
                                await awsClient.TestConnectionAsync();
                            }
                            break;

                        case PeriodicBackupConnectionType.Glacier:
                            var glacierSettings = JsonDeserializationClient.GlacierSettings(connectionInfo);
                            using (var glacierClient = new RavenAwsGlacierClient(glacierSettings, RequestHandler.ServerStore.Configuration.Backup, cancellationToken: RequestHandler.ServerStore.ServerShutdown))
                            {
                                await glacierClient.TestConnectionAsync();
                            }
                            break;

                        case PeriodicBackupConnectionType.Azure:
                            var azureSettings = JsonDeserializationClient.AzureSettings(connectionInfo);
                            using (var azureClient = RavenAzureClient.Create(azureSettings, RequestHandler.ServerStore.Configuration.Backup, cancellationToken: RequestHandler.ServerStore.ServerShutdown))
                            {
                                await azureClient.TestConnectionAsync();
                            }
                            break;

                        case PeriodicBackupConnectionType.GoogleCloud:
                            var googleCloudSettings = JsonDeserializationClient.GoogleCloudSettings(connectionInfo);
                            using (var googleCloudClient = new RavenGoogleCloudClient(googleCloudSettings, RequestHandler.ServerStore.Configuration.Backup, cancellationToken: RequestHandler.ServerStore.ServerShutdown))
                            {
                                await googleCloudClient.TestConnection();
                            }
                            break;

                        case PeriodicBackupConnectionType.FTP:
                            var ftpSettings = JsonDeserializationClient.FtpSettings(connectionInfo);
                            using (var ftpClient = new RavenFtpClient(ftpSettings))
                            {
                                ftpClient.TestConnection();
                            }
                            break;

                        case PeriodicBackupConnectionType.Local:
                        case PeriodicBackupConnectionType.None:
                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    result = new DynamicJsonValue
                    {
                        [nameof(NodeConnectionTestResult.Success)] = true,
                    };
                }
                catch (Exception e)
                {
                    result = new DynamicJsonValue
                    {
                        [nameof(NodeConnectionTestResult.Success)] = false,
                        [nameof(NodeConnectionTestResult.Error)] = e.ToString()
                    };
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    context.Write(writer, result);
                }
            }
        }
    }
}
