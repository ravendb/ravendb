using System;
using System.Security.Principal;

using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Connections
{
    public class WebSocketRequest
    {
        public string Id { get; set; }

        public Uri Uri { get; set; }

        public IResourceStore ActiveResource { get; set; }

        public string ResourceName { get; set; }

        public IPrincipal User { get; set; }

        public string Token { get; set; }
    }
}
