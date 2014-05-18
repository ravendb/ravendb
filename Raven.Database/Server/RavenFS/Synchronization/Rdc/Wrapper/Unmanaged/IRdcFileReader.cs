using System;
using System.Runtime.InteropServices;

namespace Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper.Unmanaged
{
	[InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
	[Guid("96236A74-9DBC-11DA-9E3F-0011114AE311")]
	[ComImport]
	public interface IRdcFileReader
	{
		void GetFileSize([Out] out UInt64 fileSize);

		void Read([In] UInt64 offsetFileStart, uint bytesToRead, [In, Out] ref uint bytesRead,
				  [In] IntPtr buffer, [In, Out] ref bool eof);

		void GetFilePosition(out UInt64 offsetFromStart);
	}
}