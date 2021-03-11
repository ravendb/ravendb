// -----------------------------------------------------------------------
//  <copyright file="AdminDatabasesHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Raven.Server.Routing;

namespace Raven.Server.Web.System
{
    public class EmptyMessageHandler : RequestHandler
    {
        [RavenAction("/test/empty-message", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public Task Get()
        {
            return Task.CompletedTask;
        }

        [RavenAction("/test/delay", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public Task Delay()
        {
            var delay = GetIntValueQueryString("value") ?? 0;

            return Task.Delay(delay);
        }

        [RavenAction("/test/sized-message", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetBuffer()
        {
            var buffer = LazyBuffer.Value;
            var count = GetIntValueQueryString("count", false) ?? 1;
            for (int i = 0; i < count; i++)
            {
                await HttpContext.Response.Body.WriteAsync(buffer, 0, buffer.Length);
            }
        }

        public static Lazy<byte[]> LazyBuffer = new Lazy<byte[]>(() =>
        {
            var random = new Random();
            var buffer = new byte[1024 * 32];
            random.NextBytes(buffer);
            return buffer;
        });
    }
}
