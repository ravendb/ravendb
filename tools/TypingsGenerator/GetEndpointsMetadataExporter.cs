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
            // First, check if the RavenActionAttribute has a Description property set
            var actionAttribute = methodInfo.GetCustomAttribute<RavenActionAttribute>();
            if (actionAttribute != null && !string.IsNullOrWhiteSpace(actionAttribute.Description))
            {
                return actionAttribute.Description;
            }

            // Fallback to auto-generated descriptions
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
            if (attr == null)
            {
                // This should not happen as we filter for methods with this attribute
                // But being defensive in case of future changes
                return string.Empty;
            }
            return attr.Path;
        }

        private List<QueryParameter> GetKnownParametersForPath(string path, string methodName)
        {
            var parameters = new List<QueryParameter>();

            // Index endpoints
            if (path.Contains("/indexes/terms"))
            {
                parameters.Add(new QueryParameter { Name = "name", Required = true, Description = "Name of index." });
                parameters.Add(new QueryParameter { Name = "field", Required = true, Description = "Index field to extract terms from." });
                parameters.Add(new QueryParameter { Name = "fromValue", Required = false, Description = "Starting value for term enumeration." });
                parameters.Add(new QueryParameter { Name = "pageSize", Required = false, Description = "Maximum number of terms to return." });
                parameters.Add(new QueryParameter { Name = "collection", Required = false, Description = "Collection name for dynamic index matching." });
            }
            else if (path.Contains("/indexes/staleness"))
            {
                parameters.Add(new QueryParameter { Name = "name", Required = true, Description = "Name of index." });
            }
            else if (path.Contains("/indexes/stats"))
            {
                parameters.Add(new QueryParameter { Name = "name", Required = false, Description = "Filter by specific index name." });
            }
            else if (path.Contains("/indexes/errors"))
            {
                parameters.Add(new QueryParameter { Name = "name", Required = false, Description = "Filter by specific index name." });
            }
            else if (path.Contains("/indexes/performance"))
            {
                parameters.Add(new QueryParameter { Name = "name", Required = false, Description = "Filter by specific index name." });
            }
            else if (path.Contains("/indexes/progress"))
            {
                parameters.Add(new QueryParameter { Name = "name", Required = false, Description = "Filter by specific index name." });
            }
            else if (path.Contains("/indexes/status"))
            {
                parameters.Add(new QueryParameter { Name = "name", Required = false, Description = "Filter by specific index name." });
            }
            else if (path.Contains("/indexes/source"))
            {
                parameters.Add(new QueryParameter { Name = "name", Required = true, Description = "Name of index." });
            }
            else if (path.Contains("/indexes") && methodName == "GetAll")
            {
                parameters.Add(new QueryParameter { Name = "start", Required = false, Description = "Number of results to skip." });
                parameters.Add(new QueryParameter { Name = "pageSize", Required = false, Description = "Maximum number of results to return." });
                parameters.Add(new QueryParameter { Name = "namesOnly", Required = false, Description = "Return only index names." });
            }

            // Stats and metrics endpoints
            if (path.Contains("/stats/detailed") || path.Contains("/stats/essential") || path.EndsWith("/stats"))
            {
                parameters.Add(new QueryParameter { Name = "debugInfo", Required = false, Description = "Include debug information." });
            }
            else if (path.Contains("/metrics/puts") || path.Contains("/metrics/bytes"))
            {
                parameters.Add(new QueryParameter { Name = "empty", Required = false, Description = "Include empty metrics." });
            }

            // Query endpoints
            if (path.Contains("/queries") && methodName == "Get")
            {
                parameters.Add(new QueryParameter { Name = "query", Required = false, Description = "RQL query string." });
                parameters.Add(new QueryParameter { Name = "start", Required = false, Description = "Number of results to skip." });
                parameters.Add(new QueryParameter { Name = "pageSize", Required = false, Description = "Maximum number of results to return." });
                parameters.Add(new QueryParameter { Name = "disableAutoIndexCreation", Required = false, Description = "Disable automatic index creation." });
                parameters.Add(new QueryParameter { Name = "allowStale", Required = false, Description = "Allow querying stale indexes." });
                parameters.Add(new QueryParameter { Name = "details", Required = false, Description = "Include detailed query information." });
            }

            // Document endpoints
            if (path.Contains("/docs") && methodName == "Get")
            {
                parameters.Add(new QueryParameter { Name = "id", Required = true, Description = "Document identifier." });
                parameters.Add(new QueryParameter { Name = "includes", Required = false, Description = "Related documents to include." });
            }
            else if (path.Contains("/docs/class"))
            {
                parameters.Add(new QueryParameter { Name = "id", Required = true, Description = "Document identifier." });
                parameters.Add(new QueryParameter { Name = "lang", Required = false, Description = "Programming language for class generation (csharp, java, etc.)." });
            }

            // Collection endpoints
            if (path.Contains("/collections/stats"))
            {
                parameters.Add(new QueryParameter { Name = "name", Required = false, Description = "Filter by collection name." });
            }
            else if (path.Contains("/collections/fields"))
            {
                parameters.Add(new QueryParameter { Name = "collection", Required = false, Description = "Collection name." });
                parameters.Add(new QueryParameter { Name = "prefix", Required = false, Description = "Field name prefix filter." });
            }

            // Replication endpoints
            if (path.Contains("/replication/conflicts"))
            {
                parameters.Add(new QueryParameter { Name = "docId", Required = false, Description = "Document ID to filter conflicts." });
                parameters.Add(new QueryParameter { Name = "pageSize", Required = false, Description = "Maximum number of results to return." });
            }
            else if (path.Contains("/pull-replication/hub/access"))
            {
                parameters.Add(new QueryParameter { Name = "name", Required = true, Description = "Pull replication hub name." });
            }

            // Debug endpoints
            if (path.Contains("/debug"))
            {
                if (path.Contains("/queries/running"))
                {
                    parameters.Add(new QueryParameter { Name = "details", Required = false, Description = "Include detailed information." });
                }
                else if (path.Contains("/memory"))
                {
                    parameters.Add(new QueryParameter { Name = "loh", Required = false, Description = "Include large object heap information." });
                }
                else if (path.Contains("/script-runners"))
                {
                    parameters.Add(new QueryParameter { Name = "detailed", Required = false, Description = "Include detailed information." });
                }
                else if (path.Contains("/info-package"))
                {
                    parameters.Add(new QueryParameter { Name = "type", Required = false, Description = "Type of debug package content." });
                    parameters.Add(new QueryParameter { Name = "timeoutInSecPerNode", Required = false, Description = "Timeout in seconds per node." });
                }
            }

            // Configuration endpoints
            if (path.Contains("/configuration"))
            {
                if (path.Contains("/client"))
                {
                    parameters.Add(new QueryParameter { Name = "inherit", Required = false, Description = "Inherit server-wide configuration." });
                }
            }

            // TCP/Network endpoints
            if (path.Contains("/tcp"))
            {
                parameters.Add(new QueryParameter { Name = "ip", Required = false, Description = "IP address filter." });
                parameters.Add(new QueryParameter { Name = "operation", Required = false, Description = "Operation type filter." });
            }

            // Admin endpoints
            if (path.Contains("/admin/"))
            {
                if (path.Contains("/cluster/node-info"))
                {
                    parameters.Add(new QueryParameter { Name = "nodeTag", Required = false, Description = "Specific node tag." });
                }
                else if (path.Contains("/cluster/observer/decisions"))
                {
                    parameters.Add(new QueryParameter { Name = "nodeTag", Required = false, Description = "Specific node tag." });
                }
                else if (path.Contains("/rachis/"))
                {
                    if (methodName.Contains("Suspend"))
                    {
                        parameters.Add(new QueryParameter { Name = "value", Required = true, Description = "Suspend value." });
                    }
                    else if (path.Contains("/add"))
                    {
                        parameters.Add(new QueryParameter { Name = "url", Required = true, Description = "Node URL." });
                        parameters.Add(new QueryParameter { Name = "tag", Required = false, Description = "Node tag." });
                        parameters.Add(new QueryParameter { Name = "watcher", Required = false, Description = "Add as watcher node." });
                        parameters.Add(new QueryParameter { Name = "maxUtilizedCores", Required = false, Description = "Maximum CPU cores to utilize." });
                    }
                }

                // Common pagination for admin list endpoints
                if (methodName.Contains("List") || methodName.Contains("GetAll") || methodName.Contains("Get"))
                {
                    if (!parameters.Any(p => p.Name == "start"))
                        parameters.Add(new QueryParameter { Name = "start", Required = false, Description = "Number of results to skip." });
                    if (!parameters.Any(p => p.Name == "pageSize"))
                        parameters.Add(new QueryParameter { Name = "pageSize", Required = false, Description = "Maximum number of results to return." });
                }
            }

            // Identity endpoints
            if (path.Contains("/identity"))
            {
                parameters.Add(new QueryParameter { Name = "name", Required = true, Description = "Identity name." });
                if (path.Contains("/next-identity"))
                {
                    // Already has name
                }
                else
                {
                    parameters.Add(new QueryParameter { Name = "force", Required = false, Description = "Force update identity value." });
                }
            }

            // Bulk insert endpoints
            if (path.Contains("/bulk-insert") || path.Contains("/bulkinsert"))
            {
                parameters.Add(new QueryParameter { Name = "skipOverwriteIfUnchanged", Required = false, Description = "Skip overwrite if document unchanged." });
            }

            // Subscription endpoints
            if (path.Contains("/subscriptions"))
            {
                if (path.Contains("/state"))
                {
                    parameters.Add(new QueryParameter { Name = "name", Required = true, Description = "Subscription name." });
                }
            }

            // Sharding endpoints
            if (path.Contains("/shard"))
            {
                parameters.Add(new QueryParameter { Name = "shardNumber", Required = false, Description = "Shard number." });
                parameters.Add(new QueryParameter { Name = "nodeTag", Required = false, Description = "Node tag." });
            }

            // Studio endpoints
            if (path.Contains("/studio/"))
            {
                if (path.Contains("/footer/stats"))
                {
                    parameters.Add(new QueryParameter { Name = "global", Required = false, Description = "Include global statistics." });
                }
            }

            // Certificate endpoints
            if (path.Contains("/certificates"))
            {
                if (path.Contains("/generate"))
                {
                    parameters.Add(new QueryParameter { Name = "validMonths", Required = false, Description = "Certificate validity in months." });
                }
            }

            // Common parameters for many endpoints
            if (!parameters.Any())
            {
                // Add common pagination if it's a list endpoint
                if (methodName.Contains("List") || methodName.Contains("GetAll"))
                {
                    parameters.Add(new QueryParameter { Name = "start", Required = false, Description = "Number of results to skip." });
                    parameters.Add(new QueryParameter { Name = "pageSize", Required = false, Description = "Maximum number of results to return." });
                }
            }

            return parameters;
        }

        private Type FindProcessorType(MethodInfo methodInfo, Type handlerType)
        {
            // This is a best-effort approach to find processor types using naming conventions
            // Note: RavenDB uses consistent naming patterns for processors, but this may not find all processors
            var methodBody = methodInfo.GetMethodBody();
            if (methodBody != null)
            {
                // Try to find processor type from the namespace pattern
                // Pattern: Handlers.X -> Handlers.Processors.X
                var processorNamespace = handlerType.Namespace?.Replace(".Handlers", ".Handlers.Processors");
                if (processorNamespace != null)
                {
                    // Pattern: XHandler.MethodName -> XProcessorForMethodName
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
            // Note: This method is kept as a stub for future enhancement
            // Extracting parameters from compiled method bodies would require:
            // 1. IL code analysis to find GetStringQueryString/GetIntValueQueryString calls
            // 2. Source code parsing using Roslyn
            // 3. Manual mapping (which we do in GetKnownParametersForPath)
            // 
            // For now, all parameter extraction is handled by GetKnownParametersForPath
            // which provides comprehensive mappings for common endpoint patterns
            
            return new List<QueryParameter>();
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
