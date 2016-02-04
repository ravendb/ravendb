using System;

using Raven.Database.Common;
using Raven.Database.Server.Controllers;

namespace Raven.Database.Server
{
    public class RequestWebApiEventArgs : EventArgs
    {
        public string TenantId { get; set; }
        public bool IgnoreRequest { get; set; }
        public RavenBaseApiController Controller { get; set; }

        public IResourceStore Resource { get; set; }

        public ResourceType ResourceType { get; set; }
    }
}
