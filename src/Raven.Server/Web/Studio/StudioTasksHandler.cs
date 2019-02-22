using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Formatting;
using NCrontab.Advanced;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations.Migration;
using Raven.Client.Util;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.Server.Config;
using Voron.Util.Settings;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

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
                    result = PathUtil.ToFullPath(path, baseDataDirectory);
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

        [RavenAction("/admin/studio-tasks/folder-path-options", "GET", AuthorizationStatus.Operator)]
        public Task GetFolderPathOptions()
        {
            var path = GetStringQueryString("path", required: false);
            var isBackupFolder = GetBoolValueQueryString("backupFolder", required: false) ?? false;

            var folderPathOptions = FolderPath.GetOptions(path, isBackupFolder, ServerStore.Configuration);
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(FolderPathOptions.List)] = TypeConverter.ToBlittableSupportedType(folderPathOptions.List)
                });
            }

            return Task.CompletedTask;
        }

        [RavenAction("/admin/studio-tasks/offline-migration-test", "GET", AuthorizationStatus.Operator)]
        public Task OfflineMigrationTest()
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
                
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(OfflineMigrationValidation.IsValid)] = isValid,
                        [nameof(OfflineMigrationValidation.ErrorMessage)] = errorMessage
                    });
                }
            }

            return Task.CompletedTask;
        }
        
        public class OfflineMigrationValidation
        {
            public bool IsValid { get; set; } 
            public string ErrorMessage { get; set; }
        }

        [RavenAction("/studio-tasks/is-valid-name", "GET", AuthorizationStatus.ValidUser)]
        public Task IsValidName()
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

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(NameValidation.IsValid)] = isValid,
                        [nameof(NameValidation.ErrorMessage)] = errorMessage
                    });

                    writer.Flush();
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/studio-tasks/format", "POST", AuthorizationStatus.ValidUser)]
        public Task Format()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var json = context.ReadForMemory(RequestBodyStream(), "studio-tasks/format");
                if (json == null)
                    throw new BadRequestException("No JSON was posted.");

                if (json.TryGet(nameof(FormattedExpression.Expression), out string expressionAsString) == false)
                    throw new BadRequestException("'Expression' property was not found.");

                if (string.IsNullOrWhiteSpace(expressionAsString))
                    return NoContent();

                using (var workspace = new AdhocWorkspace())
                {
                    var expression = SyntaxFactory
                        .ParseExpression(expressionAsString)
                        .NormalizeWhitespace();

                    var result = Formatter.Format(expression, workspace);

                    if (result.ToString().IndexOf("Could not format:", StringComparison.Ordinal) > -1)
                        throw new BadRequestException();

                    var formattedExpression = new FormattedExpression
                    {
                        Expression = result.ToString()
                    };

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, formattedExpression.ToJson());
                    }
                }
            }

            return Task.CompletedTask;
        }
        
        [RavenAction("/studio-tasks/next-cron-expression-occurrence", "GET", AuthorizationStatus.ValidUser)]
        public Task GetNextCronExpressionOccurrence()
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
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName(nameof(NextCronExpressionOccurrence.IsValid));
                    writer.WriteBool(false);
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(NextCronExpressionOccurrence.ErrorMessage));
                    writer.WriteString(e.Message);
                    writer.WriteEndObject();
                    writer.Flush();
                }

                return Task.CompletedTask;
            }

            var nextOccurrence = crontabSchedule.GetNextOccurrence(SystemTime.UtcNow.ToLocalTime());

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
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
                writer.Flush();
            }

            return Task.CompletedTask;
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
