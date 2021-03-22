using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.ServerWide.Commands;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;

namespace Raven.Server.Documents.Handlers
{
    public class SecretKeyHandler : RequestHandler
    {
        [RavenAction("/admin/secrets", "GET", AuthorizationStatus.Operator)]
        public async Task GetKeys()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var djv = new DynamicJsonValue
                {
                    ["Keys"] = new DynamicJsonArray(Server.ServerStore.GetSecretKeysNames(ctx))
                };

                await using (var writer = new AsyncBlittableJsonTextWriter(ctx, ResponseBodyStream()))
                {
                    ctx.Write(writer, djv);
                }
            }
        }

        [RavenAction("/admin/secrets/generate", "GET", AuthorizationStatus.Operator)]
        public Task LegacyGenerate()
        {
            return Generate();
        }

        [RavenAction("/secrets/generate", "GET", AuthorizationStatus.ValidUser)]
        public unsafe Task Generate()
        {
            HttpContext.Response.ContentType = "application/base64";

            var key = new byte[256 / 8];
            fixed (byte* pKey = key)
            {
                Sodium.randombytes_buf(pKey, (UIntPtr)key.Length);

                var base64 = Convert.ToBase64String(key);
                Sodium.sodium_memzero(pKey, (UIntPtr)key.Length);
                fixed (char* pBase64 = base64)
                {
                    try
                    {
                        WriteAsync(ResponseBodyStream(), base64).Wait(ServerStore.ServerShutdown);
                    }
                    finally
                    {
                        Sodium.sodium_memzero((byte*)pBase64, (UIntPtr)(base64.Length * sizeof(char)));
                    }
                }
            }
            return Task.CompletedTask;
        }

        private static async Task WriteAsync(Stream responseBodyStream, string base64)
        {
            await using (var writer = new StreamWriter(responseBodyStream))
            {
                await writer.WriteAsync(base64);
            }
        }

        [RavenAction("/admin/secrets", "POST", AuthorizationStatus.Operator)]
        public Task PutKey()
        {
            var name = GetStringQueryString("name");
            var overwrite = GetBoolValueQueryString("overwrite", required: false) ?? false;

            using (var reader = new StreamReader(HttpContext.Request.Body))
            {
                var base64 = reader.ReadToEnd();
                ServerStore.PutSecretKey(base64, name, overwrite);
            }

            HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

            return Task.CompletedTask;
        }

        [RavenAction("/admin/secrets/distribute", "POST", AuthorizationStatus.Operator)]
        public async Task DistributeKeyInCluster()
        {
            await ServerStore.EnsureNotPassiveAsync();

            var name = GetStringQueryString("name");
            var nodes = GetStringValuesQueryString("node");

            using (var reader = new StreamReader(HttpContext.Request.Body))
            {
                var base64 = await reader.ReadToEndAsync();
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                {
                    ClusterTopology clusterTopology;
                    using (ctx.OpenReadTransaction())
                        clusterTopology = ServerStore.GetClusterTopology(ctx);

                    foreach (var node in nodes)
                    {
                        if (string.IsNullOrEmpty(node))
                            continue;

                        if (string.Equals(node, "?") || string.Equals(node, ServerStore.NodeTag, StringComparison.OrdinalIgnoreCase))
                        {
                            var key = Convert.FromBase64String(base64);

                            if (key.Length != 256 / 8)
                                throw new ArgumentException($"Key size must be 256 bits, but was {key.Length * 8}", nameof(key));

                            StoreKeyLocally(name, key, ctx);
                        }
                        else
                        {
                            var url = clusterTopology.GetUrlFromTag(node);
                            if (url == null)
                                throw new InvalidOperationException($"Node {node} is not a part of the cluster, cannot send secret key.");

                            if (url.StartsWith("https:", StringComparison.OrdinalIgnoreCase) == false)
                                throw new InvalidOperationException($"Cannot put secret key for {name} on node {node} with url {url} because it is not using HTTPS");

                            await SendKeyToNodeAsync(name, base64, ctx, ServerStore, node, url).ConfigureAwait(false);
                        }
                    }
                }
            }

            HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
        }

        private static async Task SendKeyToNodeAsync(string name, string base64, JsonOperationContext ctx, ServerStore server, string node, string url)
        {
            using (var shortLived = RequestExecutor.CreateForSingleNodeWithoutConfigurationUpdates(url, name, server.Server.Certificate.Certificate, DocumentConventions.DefaultForServer))
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
                        Server.ServerStore.PutSecretKey(ctx, name, key);
                        tx.Commit();
                    }
                }
                finally
                {
                    Sodium.sodium_memzero(pKey, (UIntPtr)key.Length);
                }
        }
    }
}
