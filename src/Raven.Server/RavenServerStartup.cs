using System;
using System.Diagnostics;
using System.Text;
using Microsoft.AspNet.Builder;
using Microsoft.AspNet.Http;
using Microsoft.AspNet.WebSockets.Server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Raven.Server.Routing;

namespace Raven.Server
{
    public class RavenServerStartup
    {
        public void ConfigureServices(IServiceCollection services)
        {
        }

        public void Configure(IApplicationBuilder app, ILoggerFactory loggerfactory)
        {
            app.UseWebSockets(new WebSocketOptions
            {
                KeepAliveInterval = Debugger.IsAttached ? 
                    TimeSpan.FromHours(24) : TimeSpan.FromSeconds(30),
                ReceiveBufferSize = 4096,
            });

            var router = app.ApplicationServices.GetService<RequestRouter>();
            app.Run(async context =>
            {
                try
                {
                    //TODO: Kestrel bug https://github.com/aspnet/KestrelHttpServer/issues/617
                    //TODO: requires us to do this
                    var method = context.Request.Method.Trim();
                    
                    await router.HandlePath(context, method, context.Request.Path.Value);
                }
                catch (Exception e)
                {
                    if (context.RequestAborted.IsCancellationRequested)
                        return;

                    //TODO: special handling for argument exception (400 bad request)
                    //TODO: database not found (503)
                    //TODO: operaton cancelled (timeout)
                    //TODO: Invalid data exception 422

                    var response = context.Response;
                    response.StatusCode = 500;
                    var sb = new StringBuilder();
                    sb.Append(context.Request.Path).Append('?').Append(context.Request.QueryString)
                        .AppendLine()
                        .Append("- - - - - - - - - - - - - - - - - - - - -")
                        .AppendLine();
                    sb.Append(e);
                    await response.WriteAsync(e.ToString());
                }
            });
        }
    }
}