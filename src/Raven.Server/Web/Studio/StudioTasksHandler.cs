using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Formatting;
using Raven.Client;
using Raven.Client.Exceptions;
using Raven.Server.Routing;
using Sparrow.Json;

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

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteStartObject();
                        writer.WritePropertyName("Expression");
                        writer.WriteString(result.ToString());

                        writer.WriteEndObject();
                    }
                }
            }

            return Task.CompletedTask;
        }
    }
}
