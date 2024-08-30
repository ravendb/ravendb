using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Server.Commercial;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.Studio;

public sealed class UpgradeInfoHandler : ServerRequestHandler
{
    private const string ErrorPropertyName = "Error";
    
    [RavenAction("/studio/upgrade-info", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetUpgradeInfo()
    {
        if (ServerVersion.Build < 60_000)
        {
            NoContentStatus();
            return;
        }

        var start = GetStart();
        var pageSize = GetPageSize();

        using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
        {
            if (start < 0 || pageSize <= 0)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                
                context.Write(writer,
                    new DynamicJsonValue { [ErrorPropertyName] = $"Incorrect values of query string parameters - '{StartParameter}' cannot be negative, '{PageSizeParameter}' has to be greater than zero." });
                return;
            }
            
            var pageNumber = (start / pageSize) + 1;

            var licenseId = ServerStore.LicenseManager.LicenseStatus.Id.ToString();

            var request = new UpgradeInfoRequest()
            {
                UserFullVersion = ServerVersion.FullVersion,
                UserVersion = ServerVersion.Version,
                LicenseId = licenseId,
                ChangelogPageNumber = pageNumber,
                ChangelogPageSize = pageSize
            };

            var requestPayload = JsonConvert.SerializeObject(request);

            var content = new StringContent(requestPayload, Encoding.UTF8, "application/json");

            try
            {
                var upgradeInfoResponse = await ApiHttpClient.Instance.PostAsync("api/v1/upgrade/info", content);

                if (upgradeInfoResponse.IsSuccessStatusCode == false)
                {
                    HttpContext.Response.StatusCode = (int)upgradeInfoResponse.StatusCode;
                    return;
                }

                var upgradeInfoContentStream = await upgradeInfoResponse.Content.ReadAsStreamAsync();
                
                var json = await context.ReadForMemoryAsync(upgradeInfoContentStream, "about-view/data");
                var response = JsonDeserializationServer.UpgradeInfoResponse(json);

                context.Write(writer, response.ToJson());
            }
            catch (Exception e)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                context.Write(writer, new DynamicJsonValue { [ErrorPropertyName] = e.Message });
            }
        }
    }

    private class UpgradeInfoRequest
    {
        public string UserFullVersion { get; set; }
        public string UserVersion { get; set; }
        public string LicenseId { get; set; }
        public int ChangelogPageSize { get; set; }
        public int ChangelogPageNumber { get; set; }
    }
    
    public class BuildCompatibilityInfo : IDynamicJsonValueConvertible
    {
        public string FullVersion { get; set; }
        public bool CanDowngradeFollowingUpgrade { get; set; }
        public string ChangelogHtml { get; set; }
        public DateTime? ReleasedAt { get; set; }
        public bool CanUpgrade { get; set; }
        
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(FullVersion)] = FullVersion,
                [nameof(CanDowngradeFollowingUpgrade)] = CanDowngradeFollowingUpgrade,
                [nameof(ChangelogHtml)] = ChangelogHtml,
                [nameof(ReleasedAt)] = ReleasedAt,
                [nameof(CanUpgrade)] = CanUpgrade
            };
        }
    }

    public class UpgradeInfoResponse
    {
        public List<BuildCompatibilityInfo> BuildCompatibilitiesForUserMajorMinor { get; set; }
        public List<BuildCompatibilityInfo> BuildCompatibilitiesForLatestMajorMinor { get; set; }
        public long TotalBuildsForUserMajorMinor { get; set; }
        public long TotalBuildsForLatestMajorMinor { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(BuildCompatibilitiesForUserMajorMinor)] = BuildCompatibilitiesForUserMajorMinor,
                [nameof(BuildCompatibilitiesForLatestMajorMinor)] = BuildCompatibilitiesForLatestMajorMinor,
                [nameof(TotalBuildsForUserMajorMinor)] = TotalBuildsForUserMajorMinor,
                [nameof(TotalBuildsForLatestMajorMinor)] = TotalBuildsForLatestMajorMinor
            };
        }
    }
}
