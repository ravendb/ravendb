using System;
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
    [RavenAction("/studio/upgrade-info", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetUpgradeInfo()
    {
        if (ServerVersion.Build == ServerVersion.DevBuildNumber)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
            return;
        }
            
        var licenseId = ServerStore.LoadLicense()?.Id.ToString();
        
        var request = new UpgradeInfoRequest() { CurrentFullVersion = ServerVersion.FullVersion, LicenseId = licenseId };

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
            
            using (var context = JsonOperationContext.ShortTermSingleUse())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var json = await context.ReadForMemoryAsync(upgradeInfoContentStream, "about-view/data");
                var response = JsonDeserializationServer.UpgradeInfoResponse(json);

                context.Write(writer, response.ToJson());
            }
        }
        catch (Exception e)
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var response = new UpgradeInfoResponse() { ErrorMessage = e.Message };

                context.Write(writer, response.ToJson());
            }
        }
    }

    private class UpgradeInfoRequest
    {
        public string CurrentFullVersion { get; set; }
        public string LicenseId { get; set; }
    }

    public class UpgradeInfoResponse
    {
        public string ChangelogHtml { get; set; }
        public bool CanDowngradeFollowingUpgrade { get; set; }
        public bool IsLicenseEligibleForUpgrade { get; set; }
        public string ErrorMessage { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ChangelogHtml)] = ChangelogHtml,
                [nameof(CanDowngradeFollowingUpgrade)] = CanDowngradeFollowingUpgrade,
                [nameof(IsLicenseEligibleForUpgrade)] = IsLicenseEligibleForUpgrade,
                [nameof(ErrorMessage)] = ErrorMessage
            };
        }
    }
}
