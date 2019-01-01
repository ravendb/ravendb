using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Formatting;
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

namespace Raven.Server.Web.Studio
{
    public class StudioTasksHandler : RequestHandler
    {
        // return the calculated full data directory for the database before it is created according to the name & path supplied
        [RavenAction("/admin/studio-tasks/full-data-directory", "GET", AuthorizationStatus.Operator)]
        public Task FullDataDirectory()
        {
            var path = GetStringQueryString("path", required: false);
            var name = GetStringQueryString("name", required: false);

            var baseDataDirectory = ServerStore.Configuration.Core.DataDirectory.FullPath;
            var freeSpace = Size.Humane(new DriveInfo(Path.GetPathRoot(baseDataDirectory)).TotalFreeSpace);

            // 1. Used as default when both Name & Path are Not defined
            var result = baseDataDirectory;

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
                    result = $"The administrator has restricted databases to be created only under the {RavenConfiguration.GetKey(x => x.Core.DataDirectory)} directory: '{ServerStore.Configuration.Core.DataDirectory.FullPath}'.";
                }
            }
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("PathDetails");
                writer.WriteStartArray();
                writer.WriteString(result);
                writer.WriteComma();
                writer.WriteString(freeSpace);
                writer.WriteEndArray();
                writer.WriteEndObject();
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

                if (json.TryGet("Expression", out string expressionAsString) == false)
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
