using System;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.RavenFS;

namespace Raven.Database.Server
{
	public class BeforeRequestEventArgs : EventArgs
	{
		public string TenantId { get; set; }
		public DocumentDatabase Database { get; set; }
		public IHttpContext Context { get; set; }
		public bool IgnoreRequest { get; set; }
	}

	public class BeforeRequestWebApiEventArgs : EventArgs
	{
		public string TenantId { get; set; }
		public DocumentDatabase Database { get; set; }
        public RavenFileSystem FileSystem { get; set; }
        public bool IgnoreRequest { get; set; }
        public RavenBaseApiController Controller { get; set; }
	}
}