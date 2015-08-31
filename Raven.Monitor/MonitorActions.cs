using System;

namespace Raven.Monitor
{
	[Flags]
	internal enum MonitorActions
	{
		None=1,
		DiskIo =2,
		Cpu = 4,
		Memory = 8
	}
}