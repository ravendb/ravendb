using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
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
        [RavenAction("/databases/*/studio/index-type", "POST", AuthorizationStatus.ValidUser)]
        public async Task PostIndexType()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                using (var json = await context.ReadForMemoryAsync(RequestBodyStream(), "map"))
                {
                    var indexDefinition = JsonDeserializationServer.IndexDefinition(json);

                    var indexType = indexDefinition.DetectStaticIndexType();
                    
                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteStartObject();
                        writer.WritePropertyName("IndexType");
                        writer.WriteString(indexType.ToString());
                        writer.WriteEndObject();;
                    }
                }
            }
        }
        
        [RavenAction("/databases/*/studio/index-fields", "POST", AuthorizationStatus.ValidUser)]
        public async Task PostIndexFields()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                using (var json = await context.ReadForMemoryAsync(RequestBodyStream(), "map"))
                {
                    if (json.TryGet("Map", out string map) == false)
                        throw new ArgumentException("'Map' field is mandatory, but wasn't specified");

                    json.TryGet(nameof(IndexDefinition.AdditionalSources), out BlittableJsonReaderObject additionalSourcesJson);

                    var indexDefinition = new IndexDefinition
                    {
                        Name = "index-fields",
                        Maps =
                        {
                            map
                        },
                        AdditionalSources = ConvertToAdditionalSources(additionalSourcesJson)
                    };

                    try
                    {
                        var compiledIndex = IndexCompilationCache.GetIndexInstance(indexDefinition, Database.Configuration);

                        var outputFields = compiledIndex.OutputFields;

                        using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                        {
                            writer.WriteStartObject();
                            writer.WriteArray(context, "Results", outputFields, (w, c, field) => { w.WriteString(field); });
                            writer.WriteEndObject();
                        }
                    }
                    catch (IndexCompilationException)
                    {
                        // swallow compilation exception and return empty array as response
                        using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                        {
                            writer.WriteStartArray();
                            writer.WriteEndArray();
                        }
                    }
                }

            }
        }

        private static Dictionary<string, string> ConvertToAdditionalSources(BlittableJsonReaderObject json)
        {
            if (json == null || json.Count == 0)
                return null;

            var result = new Dictionary<string, string>();

            BlittableJsonReaderObject.PropertyDetails propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
            foreach (var propertyIndex in json.GetPropertiesByInsertionOrder())
            {
                json.GetPropertyByIndex(propertyIndex, ref propertyDetails);

                result[propertyDetails.Name] = propertyDetails.Value?.ToString();
            }

            return result;
        }
    }
}

