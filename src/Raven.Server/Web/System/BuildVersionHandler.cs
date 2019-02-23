// -----------------------------------------------------------------------
//  <copyright file="BuildVersion.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.BackgroundTasks;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System
{
    public class BuildVersionHandler : RequestHandler
    {
        private static readonly Lazy<byte[]> VersionBuffer = new Lazy<byte[]>(GetVersionBuffer);

        private static DateTime? _lastRunAt;

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
                        [nameof(BuildNumber.FullVersion)] = ServerVersion.FullVersion
                    });
                }
                var versionBuffer = stream.ToArray();
                return versionBuffer;
            }
        }

        [RavenAction("/build/version", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = true)]
        public async Task Get()
        {
            HttpContext.Response.Headers.Add(Constants.Headers.ServerStartupTime, ServerStore.Server.Statistics.StartUpTime.GetDefaultRavenFormat(isUtc: true));

            var versionBuffer = VersionBuffer.Value;
            await ResponseBodyStream().WriteAsync(versionBuffer, 0, versionBuffer.Length);
        }

        [RavenAction("/build/version/updates", "POST", AuthorizationStatus.ValidUser)]
        public async Task GetVersionUpdatesInfo()
        {
            var shouldRefresh = GetBoolValueQueryString("refresh", required: false) ?? false;
            if (shouldRefresh && IsLatestVersionCheckThrottled() == false)
            {
                await LatestVersionCheck.Instance.PerformAsync();
                _lastRunAt = SystemTime.UtcNow;
            }

            WriteVersionUpdatesInfo();
        }

        private static readonly TimeSpan LatestVersionCheckThrottlePeriod = TimeSpan.FromMinutes(3);

        private static bool IsLatestVersionCheckThrottled()
        {
            var lastRunAt = _lastRunAt;
            if (lastRunAt == null)
                return false;
            
            return SystemTime.UtcNow - lastRunAt.Value <= LatestVersionCheckThrottlePeriod;
        }

        private void WriteVersionUpdatesInfo()
        {
            var versionUpdatesInfo = LatestVersionCheck.Instance.GetLastRetrievedVersionUpdatesInfo();
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(LatestVersionCheck.VersionInfo.Version)] = versionUpdatesInfo?.Version,
                        [nameof(LatestVersionCheck.VersionInfo.PublishedAt)] = versionUpdatesInfo?.PublishedAt,
                        [nameof(LatestVersionCheck.VersionInfo.BuildNumber)] = versionUpdatesInfo?.BuildNumber
                    });
                }
            }

        }
    }
}
