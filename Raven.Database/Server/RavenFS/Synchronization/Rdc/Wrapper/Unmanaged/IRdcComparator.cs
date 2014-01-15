using System;
using System.Runtime.InteropServices;

namespace Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper.Unmanaged
{
	[InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
	[Guid("96236A77-9DBC-11DA-9E3F-0011114AE311")]
	[ComImport]
	public interface IRdcComparator
	{
		[PreserveSig]
		Int32 Process([In, MarshalAs(UnmanagedType.Bool)] bool endOfInput,
					  [In, Out, MarshalAs(UnmanagedType.Bool)] ref bool endOfOutput,
					  [In, Out] ref RdcBufferPointer inputBuffer,
					  [In, Out] ref RdcNeedPointer outputBuffer, out RdcError errorCode);
	}
}