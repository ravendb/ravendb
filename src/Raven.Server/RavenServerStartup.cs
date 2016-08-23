using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Xml;
using AsyncFriendlyStackTrace;
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

        public void Configure(IApplicationBuilder app, ILoggerFactory loggerfactory)
        {
            app.UseWebSockets(new WebSocketOptions
            {
                // TODO: KeepAlive causes "Unexpect reserved bit set" (we are sending our own hearbeats, so we do not need this)
                //KeepAliveInterval = Debugger.IsAttached ? 
                //    TimeSpan.FromHours(24) : TimeSpan.FromSeconds(30), 
                KeepAliveInterval = TimeSpan.FromHours(24),
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


                    //TODO: Proper json output, not like this
                    var response = context.Response;

                    if (response.HasStarted == false)
                        response.StatusCode = 500;

                    var sb = new StringBuilder();
                    sb.Append("{\r\n\t\"Url\":\"")
                        .Append(context.Request.Path).Append('?').Append(context.Request.QueryString)
                        .Append("\",")
                        .Append("\r\n\t\"Error\":\"");

                    string errorString;

                    try
                    {
                        errorString = e.ToAsyncString();
                    }
                    catch (Exception)
                    {
                        errorString = e.ToString();
                    }

                    sb.Append(errorString.Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("\r", "\\r").Replace("\n", "\\n"));
                    sb.Append("\"\r\n}");

                    await response.WriteAsync(sb.ToString());
                }
            });
        }
    }
}