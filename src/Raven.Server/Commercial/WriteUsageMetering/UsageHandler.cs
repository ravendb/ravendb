using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial.WriteUsageMetering
{
    public sealed class UsageHandler : ServerRequestHandler
    {
        [RavenAction("/usage", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetUsage()
        {
            var now = DateTime.UtcNow;
            var month = GetIntValueQueryString("month", required: false) ?? now.Month;
            var year = GetIntValueQueryString("year", required: false) ?? now.Year;

            var license = ServerStore.LoadLicense();
            if (license == null)
            {
                HttpContext.Response.StatusCode = (int)System.Net.HttpStatusCode.NotFound;
                return;
            }

            var body = new DynamicJsonValue
            {
                ["License"] = license.ToJson(),
                ["Month"] = month,
                ["Year"] = year
            };

            string json;
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var blittable = context.ReadObject(body, "usage-query"))
            {
                json = blittable.ToString();
            }

            using (var content = new StringContent(json, Encoding.UTF8, "application/json"))
            {
                var response = await ApiHttpClient.PostAsync(WriteUsageMeteringConstants.UsageQueryEndpointPath, content, token: HttpContext.RequestAborted);

               HttpContext.Response.StatusCode = (int)response.StatusCode;

                var contentType = response.Content.Headers.ContentType?.ToString();
                if (string.IsNullOrEmpty(contentType) == false)
                    HttpContext.Response.ContentType = contentType;

                await using (var responseStream = ResponseBodyStream())
                await using (var apiStream = await response.Content.ReadAsStreamAsync(HttpContext.RequestAborted))
                {
                    await apiStream.CopyToAsync(responseStream, HttpContext.RequestAborted);
                }
            }
        }
    }
}
