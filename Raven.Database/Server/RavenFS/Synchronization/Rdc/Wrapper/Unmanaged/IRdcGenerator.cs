using System;
using System.Runtime.InteropServices;

namespace Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper.Unmanaged
{
	[InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
	[Guid("96236A73-9DBC-11DA-9E3F-0011114AE311")]
	[ComImport]
	public interface IRdcGenerator
	{
		Int32 GetGeneratorParameters(uint level, [Out] out IRdcGeneratorParameters iGeneratorParameters);

		[PreserveSig]
		Int32 Process([In, MarshalAs(UnmanagedType.U1)] bool endOfInput,
					  [In, Out, MarshalAs(UnmanagedType.U1)] ref bool endOfOutput, [In, Out] ref RdcBufferPointer inputBuffer,
					  [In] uint depth, [In, MarshalAs(UnmanagedType.LPArray)] IntPtr[] outputBuffers,
					  [Out] out RdcError errorCode);
	}
}