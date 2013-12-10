using System;
using System.Runtime.InteropServices;

namespace Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper.Unmanaged
{
	[InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
	[Guid("96236A76-9DBC-11DA-9E3F-0011114AE311")]
	[ComImport]
	public interface IRdcSignatureReader
	{
		Int32 ReaderHeader(out RdcError errorCode);

		Int32 ReadSignatures(
			[In, Out, MarshalAs(UnmanagedType.Struct)] ref RdcSignaturePointer rdcSignaturePointer,
			[In, Out, MarshalAs(UnmanagedType.U1)] ref bool endOfOutput);
	}
}