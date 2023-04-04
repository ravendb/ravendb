using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
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

internal class DatabasesHandlerProcessorForGetRestorePoints : AbstractServerHandlerProxyReadProcessor<RestorePoints>
{
    public DatabasesHandlerProcessorForGetRestorePoints([NotNull] RequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected PeriodicBackupConnectionType GetPeriodicBackupConnectionType()
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

    protected ValueTask<BlittableJsonReaderObject> GetSettingsAsync(JsonOperationContext context) => context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "restore-info");

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

            var restorePathBlittable = await GetSettingsAsync(context);
            var restorePoints = new RestorePoints();
            var sortedList = new SortedList<DateTime, RestorePoint>(new RestorePointsBase.DescendedDateComparer());

            switch (connectionType)
            {
                case PeriodicBackupConnectionType.Local:
                    var localSettings = JsonDeserializationServer.LocalSettings(restorePathBlittable);
                    var directoryPath = localSettings.FolderPath;

                    try
                    {
                        Directory.GetLastAccessTime(directoryPath);
                    }
                    catch (UnauthorizedAccessException)
                    {
                        throw new InvalidOperationException($"Unauthorized access to path: {directoryPath}");
                    }

                    if (Directory.Exists(directoryPath) == false)
                        throw new InvalidOperationException($"Path '{directoryPath}' doesn't exist");

                    var localRestoreUtils = new LocalRestorePoints(sortedList, context);
                    await localRestoreUtils.FetchRestorePoints(directoryPath);

                    break;

                case PeriodicBackupConnectionType.S3:
                    var s3Settings = JsonDeserializationServer.S3Settings(restorePathBlittable);
                    using (var s3RestoreUtils = new S3RestorePoints(ServerStore.Configuration, sortedList, context, s3Settings))
                    {
                        await s3RestoreUtils.FetchRestorePoints(s3Settings.RemoteFolderName);
                    }

                    break;

                case PeriodicBackupConnectionType.Azure:
                    var azureSettings = JsonDeserializationServer.AzureSettings(restorePathBlittable);
                    using (var azureRestoreUtils = new AzureRestorePoints(ServerStore.Configuration, sortedList, context, azureSettings))
                    {
                        await azureRestoreUtils.FetchRestorePoints(azureSettings.RemoteFolderName);
                    }
                    break;

                case PeriodicBackupConnectionType.GoogleCloud:
                    var googleCloudSettings = JsonDeserializationServer.GoogleCloudSettings(restorePathBlittable);
                    using (var googleCloudRestoreUtils = new GoogleCloudRestorePoints(ServerStore.Configuration, sortedList, context, googleCloudSettings))
                    {
                        await googleCloudRestoreUtils.FetchRestorePoints(googleCloudSettings.RemoteFolderName);
                    }
                    break;

                default:
                    throw new ArgumentOutOfRangeException();
            }

            restorePoints.List = sortedList.Values.ToList();
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

    private class GetRestorePointsCommand : RavenCommand<RestorePoints>
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
