using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Commercial;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial.WriteUsageMetering
{
    public sealed class QuillHandler : ServerRequestHandler
    {
        [RavenAction("/quill/usage", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetUsage()
        {
            var licenseType = ServerStore.LicenseManager.LicenseStatus.Type;
            if (licenseType != LicenseType.Quill)
                throw new LicenseLimitException(LimitType.Quill, $"Usage data is only available under a Quill license, but the current license type is '{licenseType}'.");

            var license = ServerStore.LoadLicense();
            if (license == null)
                throw new LicenseLimitException(LimitType.InvalidLicense, "Usage data is only available under a Quill license, but no license is currently installed.");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                BlittableJsonReaderObject requestBody = await context.ReadForMemoryAsync(RequestBodyStream(), "usage-query");

                var modifications = new DynamicJsonValue(requestBody);
                requestBody.Modifications = modifications;
                modifications["License"] = license.ToJson();
                modifications["CertificateThumbprint"] = GetCurrentCertificate()?.Thumbprint;

                using (var token = CreateHttpRequestBoundOperationToken())
                using (var content = new StringContent(context.ReadObject(requestBody, "usage-query").ToString(), Encoding.UTF8, "application/json"))
                using (var response = await ApiHttpClient.PostAsync(WriteUsageMeteringConstants.UsageQueryEndpointPath, content, HttpCompletionOption.ResponseHeadersRead, shouldRetry: false, token: token.Token).ConfigureAwait(false))
                {
                    HttpContext.Response.StatusCode = (int)response.StatusCode;

                    var contentType = response.Content.Headers.ContentType?.ToString();
                    if (string.IsNullOrEmpty(contentType) == false)
                        HttpContext.Response.ContentType = contentType;

                    await using (var responseStream = ResponseBodyStream())
                    await using (var apiStream = await response.Content.ReadAsStreamAsync(token.Token))
                    {
                        await apiStream.CopyToAsync(responseStream, token.Token);
                    }
                }
            }
        }
    }
}
