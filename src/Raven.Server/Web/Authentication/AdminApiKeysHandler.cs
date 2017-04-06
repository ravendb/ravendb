using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Server.Operations.ApiKeys;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.Authentication
{
    public class AdminApiKeysHandler : RequestHandler
    {
        [RavenAction("/admin/api-keys", "PUT", "/admin/api-keys?name={api-key-name:string}")]
        public Task Put()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            TransactionOperationContext ctx;
            using (ServerStore.ContextPool.AllocateOperationContext(out ctx))
            {
                var apiKey = ctx.ReadForDisk(RequestBodyStream(), name);

                var errorTask = ValidateApiKeyStructure(name, apiKey);
                if (errorTask != null)
                    return errorTask;

                using (var tx = ctx.OpenWriteTransaction())
                {
                    ServerStore.Write(ctx, Constants.ApiKeys.Prefix + name, apiKey);

                    tx.Commit();
                }

                AccessToken value;
                if (Server.AccessTokensByName.TryRemove(name, out value))
                {
                    Server.AccessTokensById.TryRemove(value.Token, out value);
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                return Task.CompletedTask;
            }
        }

        [RavenAction("/admin/api-keys", "DELETE", "/admin/api-keys?name={api-key-name:string}")]
        public Task Delete()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            TransactionOperationContext ctx;
            using (ServerStore.ContextPool.AllocateOperationContext(out ctx))
            {
                using (var tx = ctx.OpenWriteTransaction())
                {
                    ServerStore.Delete(ctx, Constants.ApiKeys.Prefix + name);

                    tx.Commit();
                }

                AccessToken value;
                if (Server.AccessTokensByName.TryRemove(name, out value))
                {
                    Server.AccessTokensById.TryRemove(value.Token, out value);
                }

                return NoContent();
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
                ServerStore.Item[] apiKeys = null;
                try
                {
                    if (string.IsNullOrEmpty(name))
                        apiKeys = ServerStore
                            .StartingWith(context, Constants.ApiKeys.Prefix, start, pageSize)
                            .ToArray();
                    else
                    {
                        var key = Constants.ApiKeys.Prefix + name;
                        var apiKey = ServerStore.Read(context, key);
                        if (apiKey == null)
                        {
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            return Task.CompletedTask;
                        }

                        apiKeys = new[]
                        {
                            new ServerStore.Item
                            {
                                Data = apiKey,
                                Key = key
                            }
                        };
                    }

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))                
                    {
                        writer.WriteStartObject();
                        writer.WriteResults(context, apiKeys, (w, c, apiKey) =>
                        {
                            var username = apiKey.Key.Substring(Constants.ApiKeys.Prefix.Length);

                            apiKey.Data.Modifications = new DynamicJsonValue(apiKey.Data)
                            {
                                [nameof(NamedApiKeyDefinition.UserName)] = username
                            };

                            c.Write(w, apiKey.Data);
                        });

                        writer.WriteEndObject();
                    }

                }
                finally
                {
                    if (apiKeys != null)
                    {
                        foreach(var apiKey in apiKeys)
                            apiKey.Data?.Dispose();
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

                AccessModes mode;
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
