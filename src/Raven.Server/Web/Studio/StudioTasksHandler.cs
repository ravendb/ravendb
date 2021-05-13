using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Formatting;
using Microsoft.Extensions.Primitives;
using NCrontab.Advanced;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide.Operations.Migration;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Raven.Server.Documents.PeriodicBackup.GoogleCloud;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron.Util.Settings;

namespace Raven.Server.Web.Studio
{
    public class StudioTasksHandler : RequestHandler
    {
        // return the calculated full data directory for the database before it is created according to the name & path supplied
        [RavenAction("/admin/studio-tasks/full-data-directory", "GET", AuthorizationStatus.Operator)]
        public async Task FullDataDirectory()
        {
            var path = GetStringQueryString("path", required: false);
            var name = GetStringQueryString("name", required: false);
            var requestTimeoutInMs = GetIntValueQueryString("requestTimeoutInMs", required: false) ?? 5 * 1000;

            var baseDataDirectory = ServerStore.Configuration.Core.DataDirectory.FullPath;

            // 1. Used as default when both Name & Path are Not defined
            var result = baseDataDirectory;
            string error = null;

            try
            {
                // 2. Path defined, Path overrides any given Name
                if (string.IsNullOrEmpty(path) == false)
                {
                    result = new PathSetting(path, baseDataDirectory).FullPath;
                }

                // 3. Name defined, No path
                else if (string.IsNullOrEmpty(name) == false)
                {
                    // 'Databases' prefix is added...
                    result = RavenConfiguration.GetDataDirectoryPath(ServerStore.Configuration.Core, name, ResourceType.Database);
                }

                if (ServerStore.Configuration.Core.EnforceDataDirectoryPath)
                {
                    if (PathUtil.IsSubDirectory(result, ServerStore.Configuration.Core.DataDirectory.FullPath) == false)
                    {
                        error = $"The administrator has restricted databases to be created only " +
                                $"under the {RavenConfiguration.GetKey(x => x.Core.DataDirectory)} " +
                                $"directory: '{ServerStore.Configuration.Core.DataDirectory.FullPath}'.";
                    }
                }
            }
            catch (Exception e)
            {
                error = e.Message;
            }

            var getNodesInfo = GetBoolValueQueryString("getNodesInfo", required: false) ?? false;
            var info = new DataDirectoryInfo(ServerStore, result, name, isBackup: false, getNodesInfo, requestTimeoutInMs, ResponseBodyStream());
            await info.UpdateDirectoryResult(databaseName: null, error: error);
        }

        [RavenAction("/admin/studio-tasks/folder-path-options", "POST", AuthorizationStatus.Operator)]
        public Task GetFolderPathOptionsForOperator()
        {
            var type = GetStringValuesQueryString("type", required: false);
            var isBackupFolder = GetBoolValueQueryString("backupFolder", required: false) ?? false;
            var path = GetStringQueryString("path", required: false);

            return GetFolderPathOptionsInternal(ServerStore, type, isBackupFolder, path, RequestBodyStream, ResponseBodyStream);
        }

        internal static async Task GetFolderPathOptionsInternal(
            ServerStore serverStore,
            StringValues types,
            bool isBackupFolder, 
            string path,
            Func<Stream> requestBodyStream,
            Func<Stream> responseBodyStream)
        {
            PeriodicBackupConnectionType connectionType;
            var type = types.FirstOrDefault();
            if (type == null)
            {
                //Backward compatibility
                connectionType = PeriodicBackupConnectionType.Local;
            }
            else if (Enum.TryParse(type, out connectionType) == false)
            {
                throw new ArgumentException($"Query string '{type}' was not recognized as valid type");
            }

            using (serverStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var folderPathOptions = new FolderPathOptions();
                ;
                switch (connectionType)
                {
                    case PeriodicBackupConnectionType.Local:
                        
                        folderPathOptions = FolderPath.GetOptions(path, isBackupFolder, serverStore.Configuration);
                        break;

                    case PeriodicBackupConnectionType.S3:
                        var json = await context.ReadForMemoryAsync(requestBodyStream(), "studio-tasks/format");
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

                        using (var client = new RavenAwsS3Client(s3Settings))
                        {
                            // fetching only the first 64 results for the auto complete
                            var folders = await client.ListObjectsAsync(s3Settings.RemoteFolderName, "/", true, 64);
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
                        var azureJson = await context.ReadForMemoryAsync(requestBodyStream(), "studio-tasks/format");

                        if (connectionType != PeriodicBackupConnectionType.Local && azureJson == null)
                            throw new BadRequestException("No JSON was posted.");

                        var azureSettings = JsonDeserializationServer.AzureSettings(azureJson);
                        if (azureSettings == null)
                            throw new BadRequestException("No AzureSettings were found.");

                        if (string.IsNullOrWhiteSpace(azureSettings.AccountName) ||
                            string.IsNullOrWhiteSpace(azureSettings.AccountKey) ||
                            string.IsNullOrWhiteSpace(azureSettings.StorageContainer))
                            break;

                        using (var client = new RavenAzureClient(azureSettings))
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
                        var googleCloudJson = await context.ReadForMemoryAsync(requestBodyStream(), "studio-tasks/format");

                        if (connectionType != PeriodicBackupConnectionType.Local && googleCloudJson == null)
                            throw new BadRequestException("No JSON was posted.");

                        var googleCloudSettings = JsonDeserializationServer.GoogleCloudSettings(googleCloudJson);
                        if (googleCloudSettings == null)
                            throw new BadRequestException("No AzureSettings were found.");

                        if (string.IsNullOrWhiteSpace(googleCloudSettings.BucketName) ||
                            string.IsNullOrWhiteSpace(googleCloudSettings.GoogleCredentialsJson))
                            break;

                        using (var client = new RavenGoogleCloudClient(googleCloudSettings))
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

                await using (var writer = new AsyncBlittableJsonTextWriter(context, responseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue { [nameof(FolderPathOptions.List)] = TypeConverter.ToBlittableSupportedType(folderPathOptions.List) });
                }
            }
        }

        [RavenAction("/admin/studio-tasks/offline-migration-test", "GET", AuthorizationStatus.Operator)]
        public async Task OfflineMigrationTest()
        {
            var mode = GetStringQueryString("mode");
            var path = GetStringQueryString("path");
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                bool isValid = true;
                string errorMessage = null;

                try
                {
                    switch (mode)
                    {
                        case "dataDir":
                            OfflineMigrationConfiguration.ValidateDataDirectory(path);
                            break;

                        case "migratorPath":
                            OfflineMigrationConfiguration.ValidateExporterPath(path);
                            break;

                        default:
                            throw new BadRequestException("Unknown mode: " + mode);
                    }
                }
                catch (Exception e)
                {
                    isValid = false;
                    errorMessage = e.Message;
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(OfflineMigrationValidation.IsValid)] = isValid,
                        [nameof(OfflineMigrationValidation.ErrorMessage)] = errorMessage
                    });
                }
            }
        }

        public class OfflineMigrationValidation
        {
            public bool IsValid { get; set; }
            public string ErrorMessage { get; set; }
        }

        [RavenAction("/studio-tasks/periodic-backup/test-credentials", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task TestPeriodicBackupCredentials()
        {
            var type = GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");

            if (Enum.TryParse(type, out PeriodicBackupConnectionType connectionType) == false)
                throw new ArgumentException($"Unknown backup connection: {type}");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                DynamicJsonValue result;
                try
                {
                    var connectionInfo = await context.ReadForMemoryAsync(RequestBodyStream(), "test-connection");
                    switch (connectionType)
                    {
                        case PeriodicBackupConnectionType.S3:
                            var s3Settings = JsonDeserializationClient.S3Settings(connectionInfo);
                            using (var awsClient = new RavenAwsS3Client(s3Settings, cancellationToken: ServerStore.ServerShutdown))
                            {
                                awsClient.TestConnection();
                            }
                            break;

                        case PeriodicBackupConnectionType.Glacier:
                            var glacierSettings = JsonDeserializationClient.GlacierSettings(connectionInfo);
                            using (var glacierClient = new RavenAwsGlacierClient(glacierSettings, cancellationToken: ServerStore.ServerShutdown))
                            {
                                glacierClient.TestConnection();
                            }
                            break;

                        case PeriodicBackupConnectionType.Azure:
                            var azureSettings = JsonDeserializationClient.AzureSettings(connectionInfo);
                            using (var azureClient = new RavenAzureClient(azureSettings, cancellationToken: ServerStore.ServerShutdown))
                            {
                                azureClient.TestConnection();
                            }
                            break;

                        case PeriodicBackupConnectionType.GoogleCloud:
                            var googleCloudSettings = JsonDeserializationClient.GoogleCloudSettings(connectionInfo);
                            using (var googleCloudClient = new RavenGoogleCloudClient(googleCloudSettings, cancellationToken: ServerStore.ServerShutdown))
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

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, result);
                }
            }
        }

        [RavenAction("/studio-tasks/is-valid-name", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task IsValidName()
        {
            if (Enum.TryParse(GetQueryStringValueAndAssertIfSingleAndNotEmpty("type").Trim(), out ItemType elementType) == false)
            {
                throw new ArgumentException($"Type {elementType} is not supported");
            }

            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name").Trim();
            var path = GetStringQueryString("dataPath", false);

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                bool isValid = true;
                string errorMessage = null;

                switch (elementType)
                {
                    case ItemType.Database:
                        isValid = ResourceNameValidator.IsValidResourceName(name, path, out errorMessage);
                        break;

                    case ItemType.Index:
                        isValid = IndexStore.IsValidIndexName(name, isStatic: true, out errorMessage);
                        break;
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(NameValidation.IsValid)] = isValid,
                        [nameof(NameValidation.ErrorMessage)] = errorMessage
                    });
                }
            }
        }

        [RavenAction("/studio-tasks/admin/migrator-path", "GET", AuthorizationStatus.Operator)]
        public async Task HasMigratorPathInConfiguration()
        {
            // If the path from the configuration is defined, the Studio will block the option to set the path in the import view
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [$"Has{nameof(MigrationConfiguration.MigratorPath)}"] = Server.Configuration.Migration.MigratorPath != null
                    });
                }
            }
        }

        [RavenAction("/studio-tasks/format", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Format()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "studio-tasks/format");
                if (json == null)
                    throw new BadRequestException("No JSON was posted.");

                if (json.TryGet(nameof(FormattedExpression.Expression), out string expressionAsString) == false)
                    throw new BadRequestException("'Expression' property was not found.");

                if (string.IsNullOrWhiteSpace(expressionAsString))
                {
                    NoContentStatus();
                    return;
                }

                var type = IndexDefinitionHelper.DetectStaticIndexType(expressionAsString, reduce: null);

                FormattedExpression formattedExpression;
                switch (type)
                {
                    case IndexType.Map:
                    case IndexType.MapReduce:
                        using (var workspace = new AdhocWorkspace())
                        {
                            var expression = SyntaxFactory
                                .ParseExpression(expressionAsString)
                                .NormalizeWhitespace();

                            var result = Formatter.Format(expression, workspace);

                            if (result.ToString().IndexOf("Could not format:", StringComparison.Ordinal) > -1)
                                throw new BadRequestException();

                            formattedExpression = new FormattedExpression
                            {
                                Expression = result.ToString()
                            };
                        }
                        break;

                    case IndexType.JavaScriptMap:
                    case IndexType.JavaScriptMapReduce:
                        formattedExpression = new FormattedExpression
                        {
                            Expression = JSBeautify.Apply(expressionAsString)
                        };
                        break;

                    default:
                        throw new NotSupportedException($"Unknown index type '{type}'.");
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, formattedExpression.ToJson());
                }
            }
        }

        [RavenAction("/studio-tasks/next-cron-expression-occurrence", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetNextCronExpressionOccurrence()
        {
            var expression = GetQueryStringValueAndAssertIfSingleAndNotEmpty("expression");
            CrontabSchedule crontabSchedule;
            try
            {
                // will throw if the cron expression is invalid
                crontabSchedule = CrontabSchedule.Parse(expression);
            }
            catch (Exception e)
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName(nameof(NextCronExpressionOccurrence.IsValid));
                    writer.WriteBool(false);
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(NextCronExpressionOccurrence.ErrorMessage));
                    writer.WriteString(e.Message);
                    writer.WriteEndObject();
                }

                return;
            }

            var nextOccurrence = crontabSchedule.GetNextOccurrence(SystemTime.UtcNow.ToLocalTime());

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(NextCronExpressionOccurrence.IsValid));
                writer.WriteBool(true);
                writer.WriteComma();
                writer.WritePropertyName(nameof(NextCronExpressionOccurrence.Utc));
                writer.WriteDateTime(nextOccurrence.ToUniversalTime(), true);
                writer.WriteComma();
                writer.WritePropertyName(nameof(NextCronExpressionOccurrence.ServerTime));
                writer.WriteDateTime(nextOccurrence, false);
                writer.WriteEndObject();
            }
        }

        public class NextCronExpressionOccurrence
        {
            public bool IsValid { get; set; }

            public string ErrorMessage { get; set; }

            public DateTime Utc { get; set; }

            public DateTime ServerTime { get; set; }
        }

        public class FormattedExpression : IDynamicJson
        {
            public string Expression { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Expression)] = Expression
                };
            }
        }

        public enum ItemType
        {
            Index,
            Database
        }
    }
}
