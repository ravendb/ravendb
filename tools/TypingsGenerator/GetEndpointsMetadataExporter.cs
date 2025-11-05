using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json;
using Raven.Server;
using Raven.Server.Routing;
using Raven.Server.Web;

namespace TypingsGenerator
{
    public class GetEndpointsMetadataExporter
    {
        private const string TargetFile = "get-endpoints-metadata.json";

        public void Create(string targetDir)
        {
            var endpoints = ScanAssembly(typeof(RavenServer).Assembly);
            WriteEndpointsFile(targetDir, endpoints);
        }

        private void WriteEndpointsFile(string targetDir, List<EndpointMetadata> endpoints)
        {
            var options = new JsonSerializerOptions
            {
                WriteIndented = true,
                PropertyNamingPolicy = null // Keep PascalCase
            };

            var json = JsonSerializer.Serialize(endpoints, options);
            File.WriteAllText(Path.Combine(targetDir, TargetFile), json);
        }

        private List<EndpointMetadata> ScanAssembly(Assembly assembly)
        {
            var endpoints = new List<EndpointMetadata>();

            foreach (Type type in assembly.GetTypes())
            {
                if (type.IsSubclassOf(typeof(RequestHandler)))
                {
                    foreach (MethodInfo methodInfo in type.GetMethods())
                    {
                        foreach (var actionAttribute in methodInfo.GetCustomAttributes<RavenActionAttribute>())
                        {
                            if (actionAttribute.Method.Equals("GET", StringComparison.OrdinalIgnoreCase))
                            {
                                var endpoint = CreateEndpointMetadata(actionAttribute, methodInfo, type);
                                if (endpoint != null)
                                {
                                    endpoints.Add(endpoint);
                                }
                            }
                        }
                    }
                }
            }

            return endpoints.OrderBy(e => e.Path).ToList();
        }

        private EndpointMetadata CreateEndpointMetadata(RavenActionAttribute actionAttribute, MethodInfo methodInfo, Type handlerType)
        {
            var path = actionAttribute.Path;
            var description = GenerateDescription(methodInfo, handlerType, path);
            var tags = GenerateTags(path, handlerType);
            var queryParams = ExtractQueryParameters(methodInfo, handlerType);

            return new EndpointMetadata
            {
                Path = path,
                Method = "GET",
                Description = description,
                Tags = tags,
                QueryParams = queryParams
            };
        }

        private string GenerateDescription(MethodInfo methodInfo, Type handlerType, string path)
        {
            var methodName = methodInfo.Name;
            var handlerName = handlerType.Name.Replace("Handler", "");

            // Generate description based on method and path
            var description = $"{methodName} endpoint for {handlerName}";

            // Special cases for common patterns
            if (path.Contains("/stats"))
                return "Returns statistics information.";
            if (path.Contains("/debug"))
                return "Provides debug information.";
            if (path.Contains("/errors"))
                return "Returns error information.";
            if (path.Contains("/performance"))
                return "Returns performance metrics.";
            if (path.Contains("/progress"))
                return "Returns progress information.";
            if (path.Contains("/status"))
                return "Returns status information.";
            if (path.Contains("/terms"))
                return "Returns all terms in a specified index field for introspection or auto-complete.";
            if (path.Contains("/indexes") && methodName == "GetAll")
                return "Returns all indexes in the database.";
            if (path.Contains("/queries"))
                return "Executes a query and returns results.";
            if (path.Contains("/healthcheck"))
                return "Returns database health check status.";
            if (path.Contains("/metrics"))
                return "Returns database metrics.";
            if (path.Contains("/configuration"))
                return "Returns configuration settings.";

            return description;
        }

        private List<string> GenerateTags(string path, Type handlerType)
        {
            var tags = new List<string>();

            // Categorize based on path patterns
            if (path.Contains("/indexes") || path.Contains("/index"))
                tags.Add("Indexing");
            if (path.Contains("/replication"))
                tags.Add("Replication");
            if (path.Contains("/cluster"))
                tags.Add("Cluster");
            if (path.Contains("/stats") || path.Contains("/metrics") || path.Contains("/performance"))
                tags.Add("Diagnostics");
            if (path.Contains("/admin"))
                tags.Add("Admin");
            if (path.Contains("/debug"))
                tags.Add("Debug");
            if (path.Contains("/queries") || path.Contains("/query"))
                tags.Add("Query");
            if (path.Contains("/documents") || path.Contains("/docs"))
                tags.Add("Documents");
            if (path.Contains("/attachments"))
                tags.Add("Attachments");
            if (path.Contains("/revisions"))
                tags.Add("Revisions");
            if (path.Contains("/counters"))
                tags.Add("Counters");
            if (path.Contains("/time-series") || path.Contains("/timeseries"))
                tags.Add("TimeSeries");
            if (path.Contains("/etl"))
                tags.Add("ETL");
            if (path.Contains("/backup") || path.Contains("/restore"))
                tags.Add("Backup");
            if (path.Contains("/subscription"))
                tags.Add("Subscriptions");
            if (path.Contains("/compare-exchange"))
                tags.Add("CompareExchange");
            if (path.Contains("/configuration"))
                tags.Add("Configuration");
            if (path.Contains("/memory"))
                tags.Add("Memory");
            if (path.Contains("/network"))
                tags.Add("Network");
            if (path.Contains("/tcp"))
                tags.Add("Network");
            if (path.Contains("/certificate"))
                tags.Add("Security");
            if (path.Contains("/license"))
                tags.Add("Licensing");
            if (path.Contains("/studio"))
                tags.Add("Studio");
            if (path.Contains("/smuggler"))
                tags.Add("Import/Export");
            if (path.Contains("/migration"))
                tags.Add("Migration");
            if (path.Contains("/sorters"))
                tags.Add("Sorters");
            if (path.Contains("/analyzers"))
                tags.Add("Analyzers");

            // If no tags were assigned, add a generic one based on handler type
            if (tags.Count == 0)
            {
                var handlerName = handlerType.Name.Replace("Handler", "");
                if (!string.IsNullOrEmpty(handlerName))
                    tags.Add(handlerName);
            }

            return tags.Distinct().ToList();
        }

        private List<QueryParameter> ExtractQueryParameters(MethodInfo methodInfo, Type handlerType)
        {
            var parameters = new List<QueryParameter>();

            // Try to find the processor type and analyze it
            var processorType = FindProcessorType(methodInfo, handlerType);
            if (processorType != null)
            {
                parameters.AddRange(AnalyzeProcessorForParameters(processorType));
            }

            // Also check the handler method itself
            parameters.AddRange(AnalyzeMethodForParameters(methodInfo));

            // Add known parameters based on common patterns
            var path = GetPathFromMethod(methodInfo);
            parameters.AddRange(GetKnownParametersForPath(path, methodInfo.Name));

            return parameters.GroupBy(p => p.Name).Select(g => g.First()).ToList();
        }

        private string GetPathFromMethod(MethodInfo methodInfo)
        {
            var attr = methodInfo.GetCustomAttribute<RavenActionAttribute>();
            return attr?.Path ?? "";
        }

        private List<QueryParameter> GetKnownParametersForPath(string path, string methodName)
        {
            var parameters = new List<QueryParameter>();

            // Index terms endpoint
            if (path.Contains("/indexes/terms"))
            {
                parameters.Add(new QueryParameter { Name = "name", Required = true, Description = "Name of index." });
                parameters.Add(new QueryParameter { Name = "field", Required = true, Description = "Index field to extract terms from." });
                parameters.Add(new QueryParameter { Name = "fromValue", Required = false, Description = "Starting value for term enumeration." });
                parameters.Add(new QueryParameter { Name = "pageSize", Required = false, Description = "Maximum number of terms to return." });
            }
            // Index stats
            else if (path.Contains("/indexes/stats"))
            {
                parameters.Add(new QueryParameter { Name = "name", Required = false, Description = "Filter by specific index name." });
            }
            // Index errors
            else if (path.Contains("/indexes/errors"))
            {
                parameters.Add(new QueryParameter { Name = "name", Required = false, Description = "Filter by specific index name." });
            }
            // Index performance
            else if (path.Contains("/indexes/performance"))
            {
                parameters.Add(new QueryParameter { Name = "name", Required = false, Description = "Filter by specific index name." });
            }
            // Index progress
            else if (path.Contains("/indexes/progress"))
            {
                parameters.Add(new QueryParameter { Name = "name", Required = false, Description = "Filter by specific index name." });
            }
            // Index status
            else if (path.Contains("/indexes/status"))
            {
                parameters.Add(new QueryParameter { Name = "name", Required = false, Description = "Filter by specific index name." });
            }
            // Index source
            else if (path.Contains("/indexes/source"))
            {
                parameters.Add(new QueryParameter { Name = "name", Required = true, Description = "Name of index." });
            }
            // Indexes list
            else if (path.Contains("/indexes") && methodName == "GetAll")
            {
                parameters.Add(new QueryParameter { Name = "start", Required = false, Description = "Number of results to skip." });
                parameters.Add(new QueryParameter { Name = "pageSize", Required = false, Description = "Maximum number of results to return." });
                parameters.Add(new QueryParameter { Name = "namesOnly", Required = false, Description = "Return only index names." });
            }
            // Stats endpoints
            else if (path.Contains("/stats"))
            {
                parameters.Add(new QueryParameter { Name = "debugInfo", Required = false, Description = "Include debug information." });
            }
            // Queries
            else if (path.Contains("/queries") && methodName == "Get")
            {
                parameters.Add(new QueryParameter { Name = "query", Required = false, Description = "RQL query string." });
                parameters.Add(new QueryParameter { Name = "start", Required = false, Description = "Number of results to skip." });
                parameters.Add(new QueryParameter { Name = "pageSize", Required = false, Description = "Maximum number of results to return." });
            }
            // Documents
            else if (path.Contains("/docs") && methodName == "Get")
            {
                parameters.Add(new QueryParameter { Name = "id", Required = true, Description = "Document identifier." });
            }
            // Collections
            else if (path.Contains("/collections/stats"))
            {
                parameters.Add(new QueryParameter { Name = "name", Required = false, Description = "Filter by collection name." });
            }
            // Debug endpoints
            else if (path.Contains("/debug") && path.Contains("/queries/running"))
            {
                parameters.Add(new QueryParameter { Name = "details", Required = false, Description = "Include detailed information." });
            }
            // Configuration endpoints
            else if (path.Contains("/configuration"))
            {
                parameters.Add(new QueryParameter { Name = "key", Required = false, Description = "Configuration key to retrieve." });
            }

            // Common pagination parameters
            if (path.Contains("/admin/") && (methodName.Contains("List") || methodName.Contains("GetAll")))
            {
                if (!parameters.Any(p => p.Name == "start"))
                    parameters.Add(new QueryParameter { Name = "start", Required = false, Description = "Number of results to skip." });
                if (!parameters.Any(p => p.Name == "pageSize"))
                    parameters.Add(new QueryParameter { Name = "pageSize", Required = false, Description = "Maximum number of results to return." });
            }

            return parameters;
        }

        private Type FindProcessorType(MethodInfo methodInfo, Type handlerType)
        {
            // Look at the method body to find processor instantiation
            // This is a simplified approach - in a real implementation, you might need more sophisticated analysis
            var methodBody = methodInfo.GetMethodBody();
            if (methodBody != null)
            {
                // Try to find processor type from the namespace pattern
                var processorNamespace = handlerType.Namespace?.Replace(".Handlers", ".Handlers.Processors");
                if (processorNamespace != null)
                {
                    var processorTypeName = $"{processorNamespace}.{handlerType.Name.Replace("Handler", "")}ProcessorFor{methodInfo.Name}";
                    var processorType = handlerType.Assembly.GetType(processorTypeName);
                    if (processorType != null)
                        return processorType;
                }
            }

            return null;
        }

        private List<QueryParameter> AnalyzeProcessorForParameters(Type processorType)
        {
            var parameters = new List<QueryParameter>();

            // Analyze all methods in the processor type
            foreach (var method in processorType.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
            {
                parameters.AddRange(AnalyzeMethodForParameters(method));
            }

            // Also check base classes
            var baseType = processorType.BaseType;
            while (baseType != null && baseType != typeof(object))
            {
                foreach (var method in baseType.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
                {
                    parameters.AddRange(AnalyzeMethodForParameters(method));
                }
                baseType = baseType.BaseType;
            }

            return parameters;
        }

        private List<QueryParameter> AnalyzeMethodForParameters(MethodInfo method)
        {
            var parameters = new List<QueryParameter>();

            // This is a simplified approach - ideally we'd use Roslyn to parse the source
            // For now, we'll use reflection on common parameter extraction patterns
            
            // Common parameter names that we know about
            var knownParameters = new Dictionary<string, (bool required, string description)>
            {
                { "name", (true, "Name of the resource") },
                { "field", (true, "Field name") },
                { "start", (false, "Starting position for pagination") },
                { "pageSize", (false, "Number of items per page") },
                { "namesOnly", (false, "Return only names") },
                { "id", (true, "Document or resource identifier") },
                { "fromValue", (false, "Starting value for range queries") },
                { "collection", (false, "Collection name") },
                { "prefix", (false, "Prefix filter") },
                { "debugInfo", (false, "Include debug information") },
                { "details", (false, "Include detailed information") }
            };

            // Note: In a real implementation, we'd parse the method body or source code
            // For this simplified version, we return empty - the actual extraction would need
            // to be done through source code analysis or manual mapping

            return parameters;
        }

        public class EndpointMetadata
        {
            public string Path { get; set; }
            public string Method { get; set; }
            public string Description { get; set; }
            public List<string> Tags { get; set; }
            public List<QueryParameter> QueryParams { get; set; }
        }

        public class QueryParameter
        {
            public string Name { get; set; }
            public bool Required { get; set; }
            public string Description { get; set; }
        }
    }
}
