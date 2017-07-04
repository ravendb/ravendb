using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Json.Converters;
using Raven.Client.Server.Operations.ApiKeys;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.Authentication
{
    public class AdminApiKeysHandler : AdminRequestHandler
    {
        [RavenAction("/admin/api-keys", "PUT", "/admin/api-keys?name={api-key-name:string}")]
        public async Task Put()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            if (name.StartsWith("Raven/", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException($"Can't create api key named {name}, api keys starting with 'Raven/' are reserved");

            // one of the first admin action is to create an API key, so let
            // us also use that to indicate that we are the seed node
            ServerStore.EnsureNotPassive();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                var apiKeyJson = ctx.ReadForDisk(RequestBodyStream(), name);

                var errorTask = ValidateApiKeyStructure(name, apiKeyJson);
                if (errorTask != null)
                {
                    await errorTask;
                    return;
                }

                var apiKey = JsonDeserializationServer.ApiKeyDefinition(apiKeyJson);
                var res = await ServerStore.PutValueInClusterAsync(new PutApiKeyCommand(Constants.ApiKeys.Prefix + name, apiKey));
                await ServerStore.Cluster.WaitForIndexNotification(res.Etag);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
            }
        }

        [RavenAction("/admin/api-keys", "DELETE", "/admin/api-keys?name={api-key-name:string}")]
        public async Task Delete()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            if (name.StartsWith("Raven/", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException($"Can't delete api key named {name}, api keys starting with 'Raven/' are protected");
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                var res = await ServerStore.DeleteValueInClusterAsync(Constants.ApiKeys.Prefix + name);
                await ServerStore.Cluster.WaitForIndexNotification(res.Etag);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
            }
        }


        [RavenAction("/admin/api-keys", "GET", "/admin/api-keys")]
        public Task GetAll()
        {
            var name = GetStringQueryString("name", required: false);

            var start = GetStart();
            var pageSize = GetPageSize();

            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                Tuple<string, BlittableJsonReaderObject>[] apiKeys = null;
                try
                {
                    if (string.IsNullOrEmpty(name))
                        apiKeys = ServerStore.Cluster.ItemsStartingWith(context, Constants.ApiKeys.Prefix, start, pageSize)
                            .ToArray();
                    else
                    {
                        var key = Constants.ApiKeys.Prefix + name;
                        var apiKey = ServerStore.Cluster.Read(context, key);
                        if (apiKey == null)
                        {
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            return Task.CompletedTask;
                        }

                        apiKeys = new[]
                        {
                            Tuple.Create(key, apiKey)
                        };
                    }

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))                
                    {
                        writer.WriteStartObject();
                        writer.WriteArray(context, "Results", apiKeys, (w, c, apiKey) =>
                        {
                            var username = apiKey.Item1.Substring(Constants.ApiKeys.Prefix.Length);

                            apiKey.Item2.Modifications = new DynamicJsonValue(apiKey.Item2)
                            {
                                [nameof(NamedApiKeyDefinition.UserName)] = username
                            };

                            c.Write(w, apiKey.Item2);
                        });
                        writer.WriteEndObject();
                    }

                }
                finally
                {
                    if (apiKeys != null)
                    {
                        foreach(var apiKey in apiKeys)
                            apiKey.Item2?.Dispose();
                    }
                }
            }

            return Task.CompletedTask;
        }

        private Task ValidateApiKeyStructure(string name, BlittableJsonReaderObject apiKey)
        {
            if (name.Contains("/"))
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                return HttpContext.Response.WriteAsync("'name' query string should not contain '/' separator");
            }

            ApiKeyDefinition testStructureOfApiKey = new ApiKeyDefinition();

            if (apiKey.TryGet("Enabled", out testStructureOfApiKey.Enabled) == false)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                return HttpContext.Response.WriteAsync("'ApiKey' must include 'Enabled' property");
            }

            if (apiKey.TryGet("Secret", out testStructureOfApiKey.Secret) == false)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                return HttpContext.Response.WriteAsync("'ApiKey' must include 'Secret' property");
            }

            if (string.IsNullOrEmpty(testStructureOfApiKey.Secret))
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                return HttpContext.Response.WriteAsync("'ApiKey' must include non-empty 'Secret' property");
            }

            if (testStructureOfApiKey.Secret.Contains("/"))
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                return HttpContext.Response.WriteAsync("'Secret' string should not contain '/' separator");
            }

            if (apiKey.TryGet("ServerAdmin", out testStructureOfApiKey.ServerAdmin) == false)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                return HttpContext.Response.WriteAsync("'ApiKey' must include 'ServerAdmin' property");
            }

            BlittableJsonReaderObject accessMode;
            if (apiKey.TryGet("ResourcesAccessMode", out accessMode) == false)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                return HttpContext.Response.WriteAsync("'ApiKey' must include 'ResourcesAccessMode' property");
            }

            var prop = new BlittableJsonReaderObject.PropertyDetails();

            for (var i = 0; i < accessMode.Count; i++)
            {
                accessMode.GetPropertyByIndex(i, ref prop);

                string accessValue;
                if (accessMode.TryGet(prop.Name, out accessValue) == false)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                    return HttpContext.Response.WriteAsync($"Missing value of dbName -'{prop.Name}' property");
                }

                if (string.IsNullOrEmpty(accessValue))
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                    return HttpContext.Response.WriteAsync("'ApiKey' must include non-empty 'AccessMode' DB Name' property");
                }

                AccessMode mode;
                if (Enum.TryParse(accessValue, out mode) == false)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                    return
                        HttpContext.Response.WriteAsync(
                            $"Invalid value of dbName -'{prop.Name}' property, cannot understand: {accessValue}");
                }
            }
            return null;
        }
    }
}
