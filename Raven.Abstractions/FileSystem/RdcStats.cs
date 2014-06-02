
using System;

namespace Raven.Abstractions.FileSystem
{
	[CLSCompliant(false)]
	public class RdcStats
	{
		public uint CurrentVersion { get; set; }

		public uint MinimumCompatibleAppVersion { get; set; }
	}
}