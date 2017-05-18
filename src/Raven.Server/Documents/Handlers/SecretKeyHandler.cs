using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Client.Server.Commands;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class SecretKeyHandler : AdminRequestHandler
    {
        [RavenAction("/admin/secrets", "GET", "/admin/secrets")]
        public Task GetKeys()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var djv = new DynamicJsonValue
                {
                    ["Keys"] = new DynamicJsonArray(Server.ServerStore.GetSecretKeysNames(ctx))
                };

                using (var writer = new BlittableJsonTextWriter(ctx, ResponseBodyStream()))
                {
                    ctx.Write(writer, djv);
                }
            }
            return Task.CompletedTask;
        }

        [RavenAction("/admin/secrets/generate", "GET", "/admin/secrets/generate")]
        public unsafe Task Generate()
        {
            HttpContext.Response.ContentType = "application/base64";

            var key = new byte[256 / 8];
            fixed (byte* pKey = key)
            {
                Sodium.randombytes_buf(pKey, (UIntPtr)key.Length);

                var base64 = Convert.ToBase64String(key);
                Sodium.ZeroMemory(pKey, key.Length);
                fixed (char* pBase64 = base64)
                using (var writer = new StreamWriter(ResponseBodyStream()))
                {
                    writer.Write(base64);
                    Sodium.ZeroMemory((byte*)pBase64, base64.Length * sizeof(char));
                }
            }
            return Task.CompletedTask;
        }

        [RavenAction("/admin/secrets", "POST", "/admin/secrets?name={name:string}&overwrite={overwrite:bool|optional(false)}")]
        public unsafe Task PutKey()
        {
            var name = GetStringQueryString("name");
            var overwrite = GetBoolValueQueryString("overwrite", required: false) ?? false;

            using (var reader = new StreamReader(HttpContext.Request.Body))
            {
                var base64 = reader.ReadToEnd();
                var key = Convert.FromBase64String(base64);

                fixed (char* pBase64 = base64)
                fixed (byte* pKey = key)
                {
                    try
                    {
                        if (key.Length != 256 / 8)
                            throw new InvalidOperationException($"The size of the key must be 256 bits, but was {key.Length * 8} bits.");

                        using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                        using (var tx = ctx.OpenWriteTransaction())
                        {
                            Server.ServerStore.PutSecretKey(ctx, name, key, overwrite);
                            tx.Commit();
                        }
                    }
                    finally
                    {
                        Sodium.ZeroMemory((byte*)pBase64, base64.Length * sizeof(char));
                        Sodium.ZeroMemory(pKey, key.Length);
                    }
                }

            }
            
            HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

            return Task.CompletedTask;
        }

        [RavenAction("/admin/secrets/distribute", "POST", "/admin/secrets/distribute?name={name:string}&node={node:string|multiple}")]
        public async Task DistributeKeyInCluster()
        {
            var name = GetStringQueryString("name");
            var nodes = GetStringValuesQueryString("node", required: true);

            using (var reader = new StreamReader(HttpContext.Request.Body))
            {
                var base64 = reader.ReadToEnd();
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                {
                    ClusterTopology clusterTopology;
                    using (ctx.OpenReadTransaction())
                        clusterTopology = ServerStore.GetClusterTopology(ctx);

                    foreach (var node in nodes)
                    {
                        if (string.IsNullOrEmpty(node))
                            continue;

                        var url = clusterTopology.GetUrlFromTag(node);
                        if (url == null)
                            throw new InvalidOperationException($"Node {node} is not a part of the cluster, cannot send secret key.");

                        if (string.Equals(node, ServerStore.NodeTag, StringComparison.OrdinalIgnoreCase))
                        {
                            var key = Convert.FromBase64String(base64);

                            if (key.Length != 256 / 8)
                                throw new ArgumentException($"Key size must be 256 bits, but was {key.Length * 8}", nameof(key));
                            
                            StoreKeyLocally(name, key, ctx);
                        }
                        else
                        {
                            await SendKeyToNodeAsync(name, base64, ctx, clusterTopology, node, url).ConfigureAwait(false);
                        }
                    }
                }
            }

            HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
        }
       
        private async Task SendKeyToNodeAsync(string name, string base64, TransactionOperationContext ctx, ClusterTopology clusterTopology, string node, string url)
        {
            using (var shortLived = RequestExecutor.CreateForSingleNode(url, name, clusterTopology.ApiKey))
            {
                var command = new PutSecretKeyCommand(name, base64);
                try
                {
                    await shortLived.ExecuteAsync(command, ctx);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Failed to store secret key for {name} in Node {node}, url={url}", e);
                }

                if (command.StatusCode != HttpStatusCode.Created)
                    throw new InvalidOperationException($"Failed to store secret key for {name} in Node {node}, url={url}. StatusCode = {command.StatusCode}");
            }
        }

        private unsafe void StoreKeyLocally(string name, byte[] key, TransactionOperationContext ctx)
        {
            fixed (byte* pKey = key)
            try
            {
                using (var tx = ctx.OpenWriteTransaction())
                {
                    Server.ServerStore.PutSecretKey(ctx, name, key, false);
                    tx.Commit();
                }
            }
            finally
            {
                Sodium.ZeroMemory(pKey, key.Length);
            }
        }
    }
}