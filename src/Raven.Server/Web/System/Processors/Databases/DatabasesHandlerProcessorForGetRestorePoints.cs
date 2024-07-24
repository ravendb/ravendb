using System;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Web.System.Processors.Databases;

internal sealed class DatabasesHandlerProcessorForGetRestorePoints : AbstractServerHandlerProxyReadProcessor<RestorePoints>
{
    public DatabasesHandlerProcessorForGetRestorePoints([NotNull] RequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    private PeriodicBackupConnectionType GetPeriodicBackupConnectionType()
    {
        PeriodicBackupConnectionType connectionType;

        var type = RequestHandler.GetStringQueryString("type", required: false);
        if (type == null)
        {
            //Backward compatibility
            connectionType = PeriodicBackupConnectionType.Local;
        }
        else if (Enum.TryParse(type, out connectionType) == false)
        {
            throw new ArgumentException($"Query string '{type}' was not recognized as a valid type");
        }

        return connectionType;
    }

    private ValueTask<BlittableJsonReaderObject> GetSettingsAsync(JsonOperationContext context) => context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "restore-info");

    protected override async ValueTask<RavenCommand<RestorePoints>> CreateCommandForNodeAsync(string nodeTag, JsonOperationContext context)
    {
        var settings = await GetSettingsAsync(context);
        var type = GetPeriodicBackupConnectionType();

        return new GetRestorePointsCommand(type, settings, nodeTag);
    }

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            var connectionType = GetPeriodicBackupConnectionType();
            var settings = await GetSettingsAsync(context);

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(RequestHandler.Server.ServerStore.ServerShutdown, RequestHandler.AbortRequestToken);
            using var source = GetRestorePointsSource(context, connectionType, settings, out string path, cts.Token);

            var shardNumber = JsonDeserializationServer.LocalSettings(settings).ShardNumber;
            var restorePoints = await source.FetchRestorePoints(path, shardNumber);

            if (restorePoints.List.Count == 0)
                throw new InvalidOperationException("Couldn't locate any backup files.");

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                var blittable = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(restorePoints, context);
                context.Write(writer, blittable);
            }
        }
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<RestorePoints> command, JsonOperationContext context, OperationCancelToken token)
    {
        return RequestHandler.ServerStore.ClusterRequestExecutor.ExecuteAsync(command, context, token: token.Token);
    }

    private RestorePointsBase GetRestorePointsSource(TransactionOperationContext context, PeriodicBackupConnectionType connectionType, BlittableJsonReaderObject settings, out string path, CancellationToken token)
    {
        path = null;

        switch (connectionType)
        {
            case PeriodicBackupConnectionType.Local:
                var localSettings = JsonDeserializationServer.LocalSettings(settings);
                path = localSettings.FolderPath;
                try
                {
                    Directory.GetLastAccessTime(path);
                }
                catch (UnauthorizedAccessException)
                {
                    throw new InvalidOperationException($"Unauthorized access to path: {path}");
                }
                if (Directory.Exists(path) == false)
                    throw new InvalidOperationException($"Path '{path}' doesn't exist");
                return new LocalRestorePoints(context);
            case PeriodicBackupConnectionType.S3:
                var s3Settings = JsonDeserializationServer.S3Settings(settings);
                path = s3Settings.RemoteFolderName;
                return new S3RestorePoints(ServerStore.Configuration, context, s3Settings, token);
            case PeriodicBackupConnectionType.Azure:
                var azureSettings = JsonDeserializationServer.AzureSettings(settings);
                path = azureSettings.RemoteFolderName;
                return new AzureRestorePoints(ServerStore.Configuration, context, azureSettings, token);
            case PeriodicBackupConnectionType.GoogleCloud:
                var googleCloudSettings = JsonDeserializationServer.GoogleCloudSettings(settings);
                path = googleCloudSettings.RemoteFolderName;
                return new GoogleCloudRestorePoints(ServerStore.Configuration, context, googleCloudSettings, token);
            default:
                throw new ArgumentOutOfRangeException(nameof(connectionType));
        }
    }

    private sealed class GetRestorePointsCommand : RavenCommand<RestorePoints>
    {
        private readonly PeriodicBackupConnectionType _type;
        private readonly BlittableJsonReaderObject _settings;

        public GetRestorePointsCommand(PeriodicBackupConnectionType type, [NotNull] BlittableJsonReaderObject settings, string nodeTag)
        {
            _type = type;
            _settings = settings ?? throw new ArgumentNullException(nameof(settings));
            SelectedNodeTag = nodeTag;
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/restore/points?type={_type}";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        writer.WriteObject(_settings);

                }, DocumentConventions.DefaultForServer)
            };
        }
    }
}
