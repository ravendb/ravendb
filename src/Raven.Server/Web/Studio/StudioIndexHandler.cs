using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Exceptions.Compilation;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Compilation;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.Studio
{
    public class StudioIndexHandler : DatabaseRequestHandler
    {

        [RavenAction("/databases/*/studio/index-fields", "POST")]
        public async Task PostIndexFields()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                using (var json = await context.ReadForMemoryAsync(RequestBodyStream(), "map"))
                {
                    string map;
                    if (json.TryGet("Map", out map) == false)
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
                        var compiledIndex = IndexAndTransformerCompiler.Compile(indexDefinition);

                        var outputFields = compiledIndex.OutputFields;

                        using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                        {
                            writer.WriteArray(context, outputFields, (w, c, field) =>
                            {
                                w.WriteString(field);
                            });
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