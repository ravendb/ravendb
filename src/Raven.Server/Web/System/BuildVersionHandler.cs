// -----------------------------------------------------------------------
//  <copyright file="BuildVersion.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Client.Server.Operations;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System
{
    public class BuildVersionHandler : RequestHandler
    {
        private static readonly Lazy<byte[]> VersionBuffer = new Lazy<byte[]>(GetVersionBuffer);

        private static byte[] GetVersionBuffer()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var stream = new MemoryStream();
                using (var writer = new BlittableJsonTextWriter(context, stream))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(BuildNumber.BuildVersion)] = ServerVersion.Build,
                        [nameof(BuildNumber.ProductVersion)] = ServerVersion.Version,
                        [nameof(BuildNumber.CommitHash)] = ServerVersion.CommitHash,
                        [nameof(BuildNumber.FullVersion)] = ServerVersion.FullVersion,
                    });
                }
                var versionBuffer = stream.ToArray();
                return versionBuffer;
            }
        }

        [RavenAction("/build/version", "GET", "build-version", NoAuthorizationRequired = true)]
        public async Task Get()
        {
            var versionBuffer = VersionBuffer.Value;
            await ResponseBodyStream().WriteAsync(versionBuffer, 0, versionBuffer.Length);
        }
    }
}