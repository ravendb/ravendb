using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;

namespace Voron.Impl
{
	public static unsafe class MemoryMapNativeMethods
	{
		public static readonly IntPtr INVALID_HANDLE_VALUE = new IntPtr(-1);

		[Flags]
		public enum FileMapProtection : uint
		{
			PageReadonly = 0x02,
			PageReadWrite = 0x04,
			PageWriteCopy = 0x08,
			PageExecuteRead = 0x20,
			PageExecuteReadWrite = 0x40,
			SectionCommit = 0x8000000,
			SectionImage = 0x1000000,
			SectionNoCache = 0x10000000,
			SectionReserve = 0x4000000,
		}

		[DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
		public static extern IntPtr CreateFileMapping(
			IntPtr hFile,
			IntPtr lpFileMappingAttributes,
			FileMapProtection flProtect,
			uint dwMaximumSizeHigh,
			uint dwMaximumSizeLow,
			[MarshalAs(UnmanagedType.LPStr)] string lpName);

		// ReSharper disable UnusedMember.Local
		[Flags]
		public enum NativeFileMapAccessType : uint
		{
			Copy = 0x01,
			Write = 0x02,
			Read = 0x04,
			AllAccess = 0x08,
			Execute = 0x20,
		}
		// ReSharper restore UnusedMember.Local

		[DllImport("kernel32.dll", SetLastError = true)]
		public static extern bool UnmapViewOfFile(byte* lpBaseAddress);

		[DllImport("kernel32.dll", SetLastError = true)]
		public static extern byte* MapViewOfFileEx(IntPtr hFileMappingObject,
													NativeFileMapAccessType dwDesiredAccess,
													uint dwFileOffsetHigh,
													uint dwFileOffsetLow,
													UIntPtr dwNumberOfBytesToMap,
													byte* lpBaseAddress);


		[DllImport("kernel32.dll", SetLastError = true)]
		[return: MarshalAs(UnmanagedType.Bool)]
		public static extern bool FlushFileBuffers(SafeFileHandle hFile);


		[DllImport("kernel32.dll")]
		public static extern bool FlushViewOfFile(byte* lpBaseAddress, IntPtr dwNumberOfBytesToFlush);
	}
}
