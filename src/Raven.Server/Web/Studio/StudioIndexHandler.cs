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
        [RavenAction("/databases/*/studio/index-type", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task PostIndexType()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                using (var json = await context.ReadForMemoryAsync(RequestBodyStream(), "map"))
                {
                    var indexDefinition = JsonDeserializationServer.IndexDefinition(json);

                    var indexType = indexDefinition.DetectStaticIndexType();
                    var indexSourceType = indexDefinition.DetectStaticIndexSourceType();

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteStartObject();
                        writer.WritePropertyName(nameof(IndexTypeInfo.IndexType));
                        writer.WriteString(indexType.ToString());
                        writer.WriteComma();
                        writer.WritePropertyName(nameof(IndexTypeInfo.IndexSourceType));
                        writer.WriteString(indexSourceType.ToString());
                        writer.WriteEndObject();
                    }
                }
            }
        }

        public class IndexTypeInfo
        {
            public IndexType IndexType { get; set; }
            public IndexSourceType IndexSourceType { get; set; }
        }

        [RavenAction("/databases/*/studio/index-fields", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task PostIndexFields()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                using (var json = await context.ReadForMemoryAsync(RequestBodyStream(), "map"))
                {
                    if (json.TryGet("Map", out string map) == false)
                        throw new ArgumentException("'Map' field is mandatory, but wasn't specified");

                    json.TryGet(nameof(IndexDefinition.AdditionalSources), out BlittableJsonReaderObject additionalSourcesJson);
                    json.TryGet(nameof(IndexDefinition.AdditionalAssemblies), out BlittableJsonReaderArray additionalAssembliesArray);

                    var indexDefinition = new IndexDefinition
                    {
                        Name = "index-fields",
                        Maps =
                        {
                            map
                        },
                        AdditionalSources = ConvertToAdditionalSources(additionalSourcesJson),
                        AdditionalAssemblies = ConvertToAdditionalAssemblies(additionalAssembliesArray)
                    };

                    try
                    {
                        var compiledIndex = IndexCompilationCache.GetIndexInstance(indexDefinition, Database.Configuration);

                        var outputFields = compiledIndex.OutputFields;

                        await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                        {
                            writer.WriteStartObject();
                            writer.WriteArray(context, "Results", outputFields, (w, c, field) => w.WriteString(field));
                            writer.WriteEndObject();
                        }
                    }
                    catch (IndexCompilationException)
                    {
                        // swallow compilation exception and return empty array as response
                        await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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
            for (int i = 0; i < json.Count; i++)
            {
                json.GetPropertyByIndex(i, ref propertyDetails);

                result[propertyDetails.Name] = propertyDetails.Value?.ToString();
            }

            return result;
        }

        private static HashSet<AdditionalAssembly> ConvertToAdditionalAssemblies(BlittableJsonReaderArray jsonArray)
        {
            if (jsonArray == null || jsonArray.Length == 0)
                return null;

            var result = new HashSet<AdditionalAssembly>();

            foreach (BlittableJsonReaderObject assemblyJson in jsonArray)
            {
                var assembly = GetAssembly(assemblyJson);
                if (assembly != null)
                {
                    result.Add(assembly);
                }
            }

            return result;
        }

        private static AdditionalAssembly GetAssembly(BlittableJsonReaderObject json)
        {
            json.TryGet(nameof(AdditionalAssembly.AssemblyName), out string assemblyName);
            json.TryGet(nameof(AdditionalAssembly.AssemblyPath), out string assemblyPath);
            json.TryGet(nameof(AdditionalAssembly.PackageName), out string packageName);
            json.TryGet(nameof(AdditionalAssembly.PackageVersion), out string packageVersion);
            json.TryGet(nameof(AdditionalAssembly.PackageSourceUrl), out string packageSourceUrl);

            var usings = new HashSet<string>();
            json.TryGet(nameof(AdditionalAssembly.Usings), out BlittableJsonReaderArray usingsArray);

            if (usingsArray != null)
            {
                foreach (var item in usingsArray)
                {
                    usings.Add(item.ToString());
                }
            }

            if (string.IsNullOrWhiteSpace(assemblyName) == false)
            {
                return AdditionalAssembly.FromRuntime(assemblyName, usings);
            }

            if (string.IsNullOrWhiteSpace(assemblyPath) == false)
            {
                return AdditionalAssembly.FromPath(assemblyPath, usings);
            }

            if (string.IsNullOrWhiteSpace(packageName) == false && string.IsNullOrWhiteSpace(packageVersion) == false)
            {
                return AdditionalAssembly.FromNuGet(packageName, packageVersion, packageSourceUrl, usings);
            }

            return null;
        }
    }
}
