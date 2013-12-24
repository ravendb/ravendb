
using System;

namespace Raven.Client.RavenFS
{
	[CLSCompliant(false)]
	public class RdcStats
	{
		public uint CurrentVersion { get; set; }

		public uint MinimumCompatibleAppVersion { get; set; }
	}
}