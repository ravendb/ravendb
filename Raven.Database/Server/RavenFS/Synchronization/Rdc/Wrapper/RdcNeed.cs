using System;
using System.Runtime.InteropServices;

namespace Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper
{
	/// <summary>
	/// Make sure we align on the 8 byte boundry.
	/// </summary>
	[StructLayout(LayoutKind.Sequential, Pack = 8)]
	internal struct RdcNeed
	{
		public RdcNeedType BlockType;
		public UInt64 FileOffset;
		public UInt64 BlockLength;
	}
}