using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Xml;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NLog.Config;

using Raven.Server.Routing;
using LogManager = NLog.LogManager;

namespace Raven.Server
{
    public class RavenServerStartup
    {
        static RavenServerStartup()
        {
            SetupLoggingIfNeeded();
        }

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
                    await router.HandlePath(context, context.Request.Method, context.Request.Path.Value);
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

        private static void SetupLoggingIfNeeded()
        {
            if (File.Exists("NLog.config"))
            {
                var reader = XmlReader.Create("NLog.config");
                var config = new XmlLoggingConfiguration(reader, null); 
                LogManager.Configuration = config;
            }
        }
    }
}