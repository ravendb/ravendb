using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.Studio
{
    public class StudioIndexHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/studio/index-fields", "POST", AuthorizationStatus.ValidUser)]
        public async Task PostIndexFields()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                using (var json = await context.ReadForMemoryAsync(RequestBodyStream(), "map"))
                {
                    if (json.TryGet("Map", out string map) == false)
                        throw new ArgumentException("'Map' field is mandatory, but wasn't specified");

                    var indexDefinition = new IndexDefinition
                    {
                        Name = "index-fields",
                        Maps =
                        {
                            map
                        }
                    };

                    try
                    {
                        var compiledIndex = IndexCompiler.Compile(indexDefinition);

                        var outputFields = compiledIndex.OutputFields;

                        using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                        {
                            writer.WriteStartObject();
                            writer.WriteArray(context, "Results", outputFields, (w, c, field) =>
                            {
                                w.WriteString(field);
                            });
                            writer.WriteEndObject();
                        }
                    }
                    catch (IndexCompilationException)
                    {
                        // swallow compilaton exception and return empty array as response

                        using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                        {
                            writer.WriteStartArray();
                            writer.WriteEndArray();
                        }
                    }
                }
            }
        }
    }
}
