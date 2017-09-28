using System;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Formatting;
using Raven.Client.Exceptions;
using Raven.Server.Routing;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.Studio
{
    public class StudioTasksHandler : RequestHandler
    {
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
