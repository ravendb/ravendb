using System;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Controllers;

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
		public bool IgnoreRequest { get; set; }
		public RavenApiController Controller { get; set; }
	}
}