// -----------------------------------------------------------------------
//  <copyright file="BuildVersion.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.Routing;
using Raven.Server.ServerWide;

namespace Raven.Server.Web.System
{
    public class BuildVersion : RequestHandler
    {
        private static byte[] _versionBuffer;

        private static async Task<byte[]> GetVersionBuffer(ServerStore serverStore)
        {
            if (_versionBuffer != null)
                return _versionBuffer;
          /*  lock (typeof(BuildVersion))
            {*/
                if (_versionBuffer != null)
                    return _versionBuffer;

                RavenOperationContext context;
                using (serverStore.ContextPool.AllocateOperationContext(out context))
                {
                    var result = new DynamicJsonValue
                    {
                        ["BuildVersion"] = ServerVersion.Build,
                        ["ProductVersion"] = ServerVersion.Version,
                        ["CommitHash"] = ServerVersion.CommitHash
                    };
                    using (var doc = await context.ReadObject(result, "build/version"))
                    {
                        var memoryStream = new MemoryStream();
                        doc.WriteTo(memoryStream);
                        var versionBuffer = memoryStream.ToArray();
                        _versionBuffer = versionBuffer;
                        return versionBuffer;
                    }
                }
            //}
            }

        [Route("/build/version", "GET")]
        public async Task Get()
        {
            var versionBuffer = await GetVersionBuffer(ServerStore);
            await ResponseBodyStream().WriteAsync(versionBuffer, 0, versionBuffer.Length);
        }
    }
}