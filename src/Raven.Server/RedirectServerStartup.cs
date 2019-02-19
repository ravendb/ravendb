using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace Raven.Server
{
    public class RedirectServerStartup
    {
        public void Configure(IApplicationBuilder app, ILoggerFactory loggerFactory)
        {
            app.Use(_ => RedirectRequestHandler);
        }

        private Task RedirectRequestHandler(HttpContext context)
        {
            var uri = new UriBuilder
            {
                Scheme = "https",
                Host = context.Request.Host.Host,
                Path = context.Request.Path,
                Query = context.Request.QueryString.ToString(),
                Port = 443
            }.Uri.ToString();

            context.Response.StatusCode = 302;
            context.Response.Headers["Location"] = uri;

            return Task.CompletedTask;
        }
    }
}
