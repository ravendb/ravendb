using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Security.Permissions;
using Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper.Unmanaged;

namespace Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper
{
	[ClassInterface(ClassInterfaceType.None)]
	[Guid("96236A89-9DBC-11DA-9E3F-0011114AE311")]
	[SecurityPermission(SecurityAction.Demand, UnmanagedCode = true)]
	[ComVisible(true)]
	public class RdcFileReader : IRdcFileReader
	{
		private readonly Stream _stream;

		public RdcFileReader()
		{
		}

		public RdcFileReader(Stream stream)
		{
			_stream = stream;
		}

		public void GetFileSize(out ulong fileSize)
		{
			fileSize = (ulong)_stream.Length;
		}

		[PreserveSig]
		public void Read(ulong offsetFileStart, uint bytesToRead, ref uint bytesRead, IntPtr buffer, ref bool eof)
		{
			if (_stream.Position != (long)offsetFileStart)
				_stream.Seek((long)offsetFileStart, SeekOrigin.Begin);

			var intBuff = new Byte[bytesToRead];
			var read = 0;
			var lastRead = 0;
			do
			{
				lastRead = _stream.Read(intBuff, read, ((int)bytesToRead - read));
				read += lastRead;
			} while (lastRead != 0 && read < bytesToRead);
			bytesRead = (uint)read;
			Marshal.Copy(intBuff, 0, buffer, (int)bytesRead);
			eof = read < bytesRead;
		}

		public void GetFilePosition(out UInt64 offsetFromStart)
		{
			offsetFromStart = (uint)_stream.Position;
		}
	}
}