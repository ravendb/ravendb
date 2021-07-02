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
using Sparrow.Json.Sync;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Web.System
{
    public class BuildVersionHandler : ServerRequestHandler
    {
        private static readonly Lazy<byte[]> VersionBuffer = new Lazy<byte[]>(GetVersionBuffer);

        private static DateTime? _lastRunAt;

        private static byte[] GetVersionBuffer()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var stream = new MemoryStream())
            {
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

                return stream.ToArray();
            }
        }

        [RavenAction("/build/version", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task Get()
        {
            HttpContext.Response.Headers.Add(Constants.Headers.ServerStartupTime, ServerStore.Server.Statistics.StartUpTime.GetDefaultRavenFormat(isUtc: true));

            var versionBuffer = VersionBuffer.Value;
            await ResponseBodyStream().WriteAsync(versionBuffer, 0, versionBuffer.Length);
        }

        [RavenAction("/build/version/updates", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetVersionUpdatesInfo()
        {
            var shouldRefresh = GetBoolValueQueryString("refresh", required: false) ?? false;
            if (shouldRefresh && IsLatestVersionCheckThrottled() == false)
            {
                await LatestVersionCheck.Instance.PerformAsync();
                _lastRunAt = SystemTime.UtcNow;
            }

            await WriteVersionUpdatesInfo();
        }

        private static readonly TimeSpan LatestVersionCheckThrottlePeriod = TimeSpan.FromMinutes(3);

        private static bool IsLatestVersionCheckThrottled()
        {
            var lastRunAt = _lastRunAt;
            if (lastRunAt == null)
                return false;

            return SystemTime.UtcNow - lastRunAt.Value <= LatestVersionCheckThrottlePeriod;
        }

        private async Task WriteVersionUpdatesInfo()
        {
            var versionUpdatesInfo = LatestVersionCheck.Instance.GetLastRetrievedVersionUpdatesInfo();
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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
