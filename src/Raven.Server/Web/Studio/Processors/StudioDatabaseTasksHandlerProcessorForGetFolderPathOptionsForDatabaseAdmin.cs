using System;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Raven.Server.Documents.PeriodicBackup.GoogleCloud;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Raven.Server.Web.Http;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.Studio.Processors;

internal class StudioDatabaseTasksHandlerProcessorForGetFolderPathOptionsForDatabaseAdmin : AbstractServerHandlerProxyReadProcessor<FolderPathOptions>
{
    public StudioDatabaseTasksHandlerProcessorForGetFolderPathOptionsForDatabaseAdmin([NotNull] RequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        var connectionType = GetPeriodicBackupConnectionType();
        var path = GetPath();
        var isBackupFolder = IsBackupFolder();

        using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var folderPathOptions = new FolderPathOptions();
            ;
            switch (connectionType)
            {
                case PeriodicBackupConnectionType.Local:

                    folderPathOptions = FolderPath.GetOptions(path, isBackupFolder, ServerStore.Configuration);
                    break;

                case PeriodicBackupConnectionType.S3:
                    var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "studio-tasks/format");
                    if (connectionType != PeriodicBackupConnectionType.Local && json == null)
                        throw new BadRequestException("No JSON was posted.");

                    var s3Settings = JsonDeserializationServer.S3Settings(json);
                    if (s3Settings == null)
                        throw new BadRequestException("No S3Settings were found.");

                    if (string.IsNullOrWhiteSpace(s3Settings.AwsAccessKey) ||
                        string.IsNullOrWhiteSpace(s3Settings.AwsSecretKey) ||
                        string.IsNullOrWhiteSpace(s3Settings.BucketName) ||
                        string.IsNullOrWhiteSpace(s3Settings.AwsRegionName))
                        break;

                    using (var client = new RavenAwsS3Client(s3Settings, ServerStore.Configuration.Backup))
                    {
                        // fetching only the first 64 results for the auto complete
                        var folders = await client.ListObjectsAsync(s3Settings.RemoteFolderName, "/", true, take: 64);
                        if (folders != null)
                        {
                            foreach (var folder in folders.FileInfoDetails)
                            {
                                var fullPath = folder.FullPath;
                                if (string.IsNullOrWhiteSpace(fullPath))
                                    continue;

                                folderPathOptions.List.Add(fullPath);
                            }
                        }
                    }

                    break;

                case PeriodicBackupConnectionType.Azure:
                    var azureJson = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "studio-tasks/format");

                    if (connectionType != PeriodicBackupConnectionType.Local && azureJson == null)
                        throw new BadRequestException("No JSON was posted.");

                    var azureSettings = JsonDeserializationServer.AzureSettings(azureJson);
                    if (azureSettings == null)
                        throw new BadRequestException("No AzureSettings were found.");

                    if (string.IsNullOrWhiteSpace(azureSettings.AccountName) ||
                        string.IsNullOrWhiteSpace(azureSettings.AccountKey) ||
                        string.IsNullOrWhiteSpace(azureSettings.StorageContainer))
                        break;

                    using (var client = RavenAzureClient.Create(azureSettings, ServerStore.Configuration.Backup))
                    {
                        var folders = (await client.ListBlobsAsync(azureSettings.RemoteFolderName, "/", true));

                        foreach (var folder in folders.List)
                        {
                            var fullPath = folder.Name;
                            if (string.IsNullOrWhiteSpace(fullPath))
                                continue;

                            folderPathOptions.List.Add(fullPath);
                        }
                    }

                    break;

                case PeriodicBackupConnectionType.GoogleCloud:
                    var googleCloudJson = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "studio-tasks/format");

                    if (connectionType != PeriodicBackupConnectionType.Local && googleCloudJson == null)
                        throw new BadRequestException("No JSON was posted.");

                    var googleCloudSettings = JsonDeserializationServer.GoogleCloudSettings(googleCloudJson);
                    if (googleCloudSettings == null)
                        throw new BadRequestException("No AzureSettings were found.");

                    if (string.IsNullOrWhiteSpace(googleCloudSettings.BucketName) ||
                        string.IsNullOrWhiteSpace(googleCloudSettings.GoogleCredentialsJson))
                        break;

                    using (var client = new RavenGoogleCloudClient(googleCloudSettings, ServerStore.Configuration.Backup))
                    {
                        var folders = (await client.ListObjectsAsync(googleCloudSettings.RemoteFolderName));
                        var requestedPathLength = googleCloudSettings.RemoteFolderName.Split('/').Length;

                        foreach (var folder in folders)
                        {
                            const char separator = '/';
                            var splitted = folder.Name.Split(separator);
                            var result = string.Join(separator, splitted.Take(requestedPathLength)) + separator;

                            if (string.IsNullOrWhiteSpace(result))
                                continue;

                            folderPathOptions.List.Add(result);
                        }
                    }

                    break;

                case PeriodicBackupConnectionType.FTP:
                case PeriodicBackupConnectionType.Glacier:
                    throw new NotSupportedException();
                default:
                    throw new ArgumentOutOfRangeException();
            }

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(FolderPathOptions.List)] = TypeConverter.ToBlittableSupportedType(folderPathOptions.List)
                });
            }
        }
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<FolderPathOptions> command, JsonOperationContext context, OperationCancelToken token)
    {
        return ServerStore.ClusterRequestExecutor.ExecuteAsync(command, context, token: token.Token);
    }

    protected override ValueTask<RavenCommand<FolderPathOptions>> CreateCommandForNodeAsync(string nodeTag, JsonOperationContext context)
    {
        return new ValueTask<RavenCommand<FolderPathOptions>>(new GetFolderPathOptionsForDatabaseAdminCommand(GetPeriodicBackupConnectionType(), GetPath(), IsBackupFolder(), nodeTag));
    }

    protected PeriodicBackupConnectionType GetPeriodicBackupConnectionType()
    {
        var type = RequestHandler.GetStringQueryString("type", required: false);
        if (type == null)
        {
            //Backward compatibility
            return PeriodicBackupConnectionType.Local;
        }

        if (Enum.TryParse(type, out PeriodicBackupConnectionType connectionType))
            return connectionType;

        throw new ArgumentException($"Query string '{type}' was not recognized as valid type");
    }

    protected bool IsBackupFolder() => RequestHandler.GetBoolValueQueryString("backupFolder", required: false) ?? false;

    protected string GetPath() => RequestHandler.GetStringQueryString("path", required: false);

    protected override bool SupportsCurrentNode => true;

    private class GetFolderPathOptionsForDatabaseAdminCommand : AbstractGetFolderPathOptionsCommand
    {
        public GetFolderPathOptionsForDatabaseAdminCommand(PeriodicBackupConnectionType connectionType, string path, bool isBackupFolder, string nodeTag)
            : base(connectionType, path, isBackupFolder, nodeTag)
        {
        }

        protected override string GetBaseUrl(ServerNode node) => $"{node.Url}/databases/{node.Database}/admin/studio-tasks/folder-path-options";
    }

    protected abstract class AbstractGetFolderPathOptionsCommand : RavenCommand<FolderPathOptions>
    {
        private readonly PeriodicBackupConnectionType _connectionType;
        private readonly string _path;
        private readonly bool _isBackupFolder;
        public override bool IsReadRequest => false;

        protected AbstractGetFolderPathOptionsCommand(PeriodicBackupConnectionType connectionType, string path, bool isBackupFolder, string nodeTag)
        {
            _connectionType = connectionType;
            _path = path;
            _isBackupFolder = isBackupFolder;
            SelectedNodeTag = nodeTag;
        }

        protected abstract string GetBaseUrl(ServerNode node);

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{GetBaseUrl(node)}?type={_connectionType}&path={_path}&backupFolder={_isBackupFolder}";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Post
            };
        }
    }
}
