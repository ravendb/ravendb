using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNet.Http;
using NetTopologySuite.Noding;
using NetTopologySuite.Utilities;
using Raven.Abstractions.Data;
using Raven.Client.Data;
using Raven.Json.Linq;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.Authentication
{
    public class AdminApiKeysHandler : RequestHandler
    {
        [RavenAction("/admin/api-keys", "PUT", "/admin/api-keys?name={api-key-name:string}")]
        public Task PutApiKey()
        {
            TransactionOperationContext ctx;
            using (ServerStore.ContextPool.AllocateOperationContext(out ctx))
            {
                var name = HttpContext.Request.Query["name"];

                if (name.Count != 1)
                {
                    HttpContext.Response.StatusCode = 400;
                    return HttpContext.Response.WriteAsync("'name' query string must have exactly one value");
                }

                if (string.IsNullOrEmpty(name[0]))
                {
                    HttpContext.Response.StatusCode = 400;
                    return HttpContext.Response.WriteAsync("'name' query string must be non-empty");
                }

                var apiKey = ctx.ReadForDisk(RequestBodyStream(), name[0]);

                var errorTask = ValidateApiKeyStructure(name[0], apiKey);
                if (errorTask != null)
                    return errorTask;

                using (var tx = ctx.OpenWriteTransaction())
                {
                    ServerStore.Write(ctx, Constants.ApiKeyPrefix + name[0], apiKey);

                    tx.Commit();
                }
                AccessToken value;
                if (Server.AccessTokensByName.TryRemove(name[0], out value))
                {
                    Server.AccessTokensById.TryRemove(value.Token, out value);
                }
                return Task.CompletedTask;
            }
        }

        [RavenAction("/admin/api-keys", "GET", "/admin/api-keys?name={api-key-name:string}")]
        public Task GetApiKey()
        {
            TransactionOperationContext ctx;
            using (ServerStore.ContextPool.AllocateOperationContext(out ctx))
            {
                var name = HttpContext.Request.Query["name"];

                if (name.Count != 1)
                {
                    HttpContext.Response.StatusCode = 400;
                    return HttpContext.Response.WriteAsync("'name' query string must have exactly one value");
                }

                ctx.OpenReadTransaction();

                var apiKey = ServerStore.Read(ctx, Constants.ApiKeyPrefix + name[0]);

                if (apiKey == null)
                {
                    HttpContext.Response.StatusCode = 404;
                    return Task.CompletedTask;
                }

                HttpContext.Response.StatusCode = 200;

                ctx.Write(ResponseBodyStream(), apiKey);

                return Task.CompletedTask;
            }
        }

        [RavenAction("/admin/api-keys", "DELETE", "/admin/api-keys?name={api-key-name:string}")]
        public Task DeleteApiKey()
        {
            TransactionOperationContext ctx;
            using (ServerStore.ContextPool.AllocateOperationContext(out ctx))
            {
                var name = HttpContext.Request.Query["name"];

                if (name.Count != 1)
                {
                    HttpContext.Response.StatusCode = 400;
                    return HttpContext.Response.WriteAsync("'name' query string must have exactly one value");
                }

                using (var tx = ctx.OpenWriteTransaction())
                {
                    ServerStore.Delete(ctx, Constants.ApiKeyPrefix + name[0]);

                    tx.Commit();
                }
                AccessToken value;
                if (Server.AccessTokensByName.TryRemove(name[0], out value))
                {
                    Server.AccessTokensById.TryRemove(value.Token, out value);
                }
                return Task.CompletedTask;
            }
        }


        [RavenAction("/admin/apikeys/all", "GET", "/admin/apikeys/all")]
        public Task GetAllGetApiKey()
        {
            var start = GetStart();
            var pageSize = GetPageSize();

            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();


                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartArray();
                    bool first = true;
                    foreach (var item in ServerStore.StartingWith(context, Constants.ApiKeyPrefix, start, pageSize))
                    {
                        if (first == false)
                            writer.WriteComma();
                        else
                            first = false;

                        string username = item.Key.Substring(Constants.ApiKeyPrefix.Length);

                        item.Data.Modifications = new DynamicJsonValue(item.Data)
                        {
                            ["UserName"] = username
                        };
                        context.Write(writer, item.Data);
                    }
                    writer.WriteEndArray();

                }
            }

            return Task.CompletedTask;
        }

        private Task ValidateApiKeyStructure(string name, BlittableJsonReaderObject apiKey)
        {
            if (name.Contains("/"))
            {
                HttpContext.Response.StatusCode = 400;
                return HttpContext.Response.WriteAsync("'name' query string should not contain '/' separator");
            }

            ApiKeyDefinition testStructureOfApiKey = new ApiKeyDefinition();

            if (apiKey.TryGet("Enabled", out testStructureOfApiKey.Enabled) == false)
            {
                HttpContext.Response.StatusCode = 400;
                return HttpContext.Response.WriteAsync("'ApiKey' must include 'Enabled' property");
            }

            if (apiKey.TryGet("Secret", out testStructureOfApiKey.Secret) == false)
            {
                HttpContext.Response.StatusCode = 400;
                return HttpContext.Response.WriteAsync("'ApiKey' must include 'Secret' property");
            }

            if (string.IsNullOrEmpty(testStructureOfApiKey.Secret))
            {
                HttpContext.Response.StatusCode = 400;
                return HttpContext.Response.WriteAsync("'ApiKey' must include non-empty 'Secret' property");
            }

            if (testStructureOfApiKey.Secret.Contains("/"))
            {
                HttpContext.Response.StatusCode = 400;
                return HttpContext.Response.WriteAsync("'Secret' string should not contain '/' separator");
            }

            if (apiKey.TryGet("ServerAdmin", out testStructureOfApiKey.ServerAdmin) == false)
            {
                HttpContext.Response.StatusCode = 400;
                return HttpContext.Response.WriteAsync("'ApiKey' must include 'ServerAdmin' property");
            }

            BlittableJsonReaderObject accessMode;
            if (apiKey.TryGet("ResourcesAccessMode", out accessMode) == false)
            {
                HttpContext.Response.StatusCode = 400;
                return HttpContext.Response.WriteAsync("'ApiKey' must include 'ResourcesAccessMode' property");
            }

            for (var i = 0; i < accessMode.Count; i++)
            {
                var dbName = accessMode.GetPropertyByIndex(i);

                string accessValue;
                if (accessMode.TryGet(dbName.Item1, out accessValue) == false)
                {
                    HttpContext.Response.StatusCode = 400;
                    return HttpContext.Response.WriteAsync($"Missing value of dbName -'{dbName.Item1}' property");
                }

                if (string.IsNullOrEmpty(accessValue))
                {
                    HttpContext.Response.StatusCode = 400;
                    return HttpContext.Response.WriteAsync("'ApiKey' must include non-empty 'AccessMode' DB Name' property");
                }

                AccessModes mode;
                if (Enum.TryParse(accessValue, out mode) == false)
                {
                    HttpContext.Response.StatusCode = 400;
                    return
                        HttpContext.Response.WriteAsync(
                            $"Invalid value of dbName -'{dbName.Item1}' property, cannot understand: {accessValue}");
                }
            }
            return null;
        }
    }
}
