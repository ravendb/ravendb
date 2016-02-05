// -----------------------------------------------------------------------
//  <copyright file="BuildVersion.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.Routing;
using Raven.Server.ServerWide;

namespace Raven.Server.Web.System
{
    public class BuildVersionHandler : RequestHandler
    {
        private static readonly Lazy<Task<byte[]>> VersionBuffer = new Lazy<Task<byte[]>>(GetVersionBuffer);

        private static async Task<byte[]> GetVersionBuffer()
        {
            using (var pool = new UnmanagedBuffersPool("build/version"))
            using (var context = new RavenOperationContext(pool))
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
                    return versionBuffer;
                }
            }
        }

        [RavenAction("/build/version", "GET")]
        public async Task Get()
        {
            var versionBuffer = await VersionBuffer.Value;
            await ResponseBodyStream().WriteAsync(versionBuffer, 0, versionBuffer.Length);
        }
    }
}