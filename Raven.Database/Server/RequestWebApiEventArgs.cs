using System;
using Raven.Database.Counters;
using Raven.Database.Server.Controllers;
using Raven.Database.FileSystem;
using Raven.Database.TimeSeries;

namespace Raven.Database.Server
{
	public class RequestWebApiEventArgs : EventArgs
	{
		public string TenantId { get; set; }
		public DocumentDatabase Database { get; set; }
        public RavenFileSystem FileSystem { get; set; }
        public bool IgnoreRequest { get; set; }
        public RavenBaseApiController Controller { get; set; }
		public CounterStorage Counters { get; set; }
		public TimeSeriesStorage TimeSeries { get; set; }
	}
}