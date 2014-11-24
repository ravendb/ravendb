using System;
using Raven.Database.Counters;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Controllers;
using Raven.Database.FileSystem;

namespace Raven.Database.Server
{
	public class BeforeRequestWebApiEventArgs : EventArgs
	{
		public string TenantId { get; set; }
		public DocumentDatabase Database { get; set; }
        public RavenFileSystem FileSystem { get; set; }
        public bool IgnoreRequest { get; set; }
        public RavenBaseApiController Controller { get; set; }
		public CounterStorage Counters { get; set; }
	}
}