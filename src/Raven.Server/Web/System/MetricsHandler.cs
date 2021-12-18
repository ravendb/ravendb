using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Routing;

namespace Raven.Server.Web.System
{
    public sealed class MetricsHandler : RequestHandler
    {
        [RavenAction("/metrics", "GET", AuthorizationStatus.Operator)]
        public async Task Metrics()
        {
                HttpContext.Response.ContentType = "text/plain; version=0.0.4; charset=utf-8";

                var temp = @"# HELP raven_test_http_requests_total The total number of HTTP requests.
# TYPE raven_test_http_requests_total counter
raven_test_http_requests_total{method=""post"",code=""200""} 1027
raven_test_http_requests_total{method=""post"",code=""400""}    3";
                //TODO: you may add time column
                temp = temp.Replace("\r\n", "\n");

                var randomValue = new Random().Next(1000) + 100;
                temp = temp.Replace("1027", randomValue.ToString());

                byte[] bytes = Encoding.UTF8.GetBytes(temp);
                await using (var ms = new MemoryStream(bytes))
                {
                    await ms.CopyToAsync(ResponseBodyStream());
                }
        }
    }
}
