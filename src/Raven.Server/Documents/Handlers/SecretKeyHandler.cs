using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
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
                Sodium.randombytes_buf(pKey, key.Length);

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

        [RavenAction("/admin/secrets", "PUT", "/admin/secrets?name={name:string}&overwrite={overwrite:bool|optional(false)}")]
        public unsafe Task PutKey()
        {
            var name = GetStringQueryString("name");
            var overwrite = GetBoolValueQueryString("overwrite", required: false) ?? false;

            using (var reader = new StreamReader(HttpContext.Request.Body))
            {
                var base64 = reader.ReadToEnd();

                var key = Convert.FromBase64String(base64);

                if (key.Length != 256 / 8)
                    throw new InvalidOperationException($"The size of the key must be 256 bits, but was {key.Length * 8} bits.");
                fixed (char* pBase64 = base64)
                fixed (byte* pKey = key)
                {
                    Sodium.ZeroMemory((byte*)pBase64, base64.Length * sizeof(char));

                    using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (var tx = ctx.OpenWriteTransaction())
                    {
                        Server.ServerStore.PutSecretKey(ctx, name, key, overwrite);
                        Sodium.ZeroMemory(pKey, key.Length);

                        tx.Commit();
                    }
                }
            }


            return Task.CompletedTask;
        }
    }
}