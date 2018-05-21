using System;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Formatting;
using Raven.Client.Exceptions;
using Raven.Server.Config;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron.Util.Settings;

namespace Raven.Server.Web.Studio
{
    public class StudioTasksHandler : RequestHandler
    {
        // return the calculated full data directory for the database before it is created according to the name & path supplied
        [RavenAction("/studio-tasks/full-data-directory", "GET", AuthorizationStatus.ValidUser)]
        public Task FullDataDirectory()
        {
            var path = GetStringQueryString("path", required: false);
            var name = GetStringQueryString("name", required: false);
            
            var baseDataDirectory = ServerStore.Configuration.Core.DataDirectory.FullPath;
            
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
            
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("FullPath");
                writer.WriteString(result);
                writer.WriteEndObject();
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

                    var formatedExpression = new FormatedExpression
                    {
                        Expression = result.ToString()
                    };

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, formatedExpression.ToJson());
                    }
                }
            }

            return Task.CompletedTask;
        }

        public class FormatedExpression : IDynamicJson
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
    }
}
