using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using NCrontab.Advanced;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations.Migration;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.ETL.Providers.ElasticSearch;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.IndexMerging;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Raven.Server.Documents.PeriodicBackup.GoogleCloud;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Studio.Processors;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron.Util.Settings;

namespace Raven.Server.Web.Studio
{
    public class StudioTasksHandler : ServerRequestHandler
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
        public async Task GetFolderPathOptionsForOperator()
        {
            using (var processor = new StudioDatabaseTasksHandlerProcessorForGetFolderPathOptionsForOperator(this))
                await processor.ExecuteAsync();
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
            using (var processor = new StudioTasksHandlerProcessorForTestPeriodicBackupCredentials(this))
                await processor.ExecuteAsync();
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

                    case ItemType.Script:
                        isValid = ResourceNameValidator.IsValidFileName(name, out errorMessage);
                        break;

                    case ItemType.ElasticSearchIndex:
                        isValid = ElasticSearchIndexValidator.IsValidIndexName(name, out errorMessage);
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

                if (json.TryGet(nameof(SourceCodeBeautifier.FormattedExpression.Expression), out string expressionAsString) == false)
                    throw new BadRequestException("'Expression' property was not found.");

                if (string.IsNullOrWhiteSpace(expressionAsString))
                {
                    NoContentStatus();
                    return;
                }

                SourceCodeBeautifier.FormattedExpression formattedExpression = SourceCodeBeautifier.FormatIndex(expressionAsString);

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

        public enum ItemType
        {
            Index,
            Database,
            Script,
            ElasticSearchIndex
        }
    }
}
