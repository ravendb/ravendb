using System;
using System.Runtime.InteropServices;

namespace Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper.Unmanaged
{
	[StructLayout(LayoutKind.Sequential, Pack = 4)]
	internal struct RdcNeedPointer
	{
		public uint Size;
		public uint Used;
		public IntPtr Data;
	}
}