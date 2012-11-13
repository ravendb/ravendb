using System;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server
{
	public class BeforeRequestEventArgs : EventArgs
	{
		public string TenantId { get; set; }
		public DocumentDatabase Database { get; set; }
		public IHttpContext Context { get; set; }
		public bool IgnoreRequest { get; set; }
	}
}