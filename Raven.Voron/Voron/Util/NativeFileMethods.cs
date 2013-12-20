// -----------------------------------------------------------------------
//  <copyright file="NativeFileMethods.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Voron.Exceptions;

namespace Voron.Impl
{
	public static unsafe class NativeFileMethods
	{
		[DllImport("kernel32.dll", SetLastError = true)]
		public static extern bool WriteFile(SafeFileHandle hFile, byte* lpBuffer, int nNumberOfBytesToWrite,
		                                    out int lpNumberOfBytesWritten, NativeOverlapped* lpOverlapped);


		[DllImport(@"kernel32.dll", SetLastError = true)]
		public static extern bool ReadFile(
			SafeFileHandle hFile,
			byte* pBuffer,
			int numBytesToRead,
			out int pNumberOfBytesRead,
			NativeOverlapped* lpOverlapped
			);

		[DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
		public static extern SafeFileHandle CreateFile(string lpFileName,
		                                               NativeFileAccess dwDesiredAccess, NativeFileShare dwShareMode,
		                                               IntPtr lpSecurityAttributes,
		                                               NativeFileCreationDisposition dwCreationDisposition,
		                                               NativeFileAttributes dwFlagsAndAttributes, IntPtr hTemplateFile);

		[DllImport("kernel32.dll", SetLastError = true)]
		public static extern bool CloseHandle(IntPtr hObject);

		[DllImport("kernel32.dll", SetLastError = true)]
		private static extern bool SetEndOfFile(SafeFileHandle hFile);

		[DllImport("Kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
		private static extern int SetFilePointer([In] SafeFileHandle hFile, [In] int lDistanceToMove,
		                                         [Out] out int lpDistanceToMoveHigh, [In] NativeFileMoveMethod dwMoveMethod);

		[DllImport("kernel32.dll")]
		public static extern bool FlushFileBuffers(SafeFileHandle hFile);

		[DllImport("kernel32.dll", EntryPoint = "GetFinalPathNameByHandleW", CharSet = CharSet.Unicode, SetLastError = true)]
		public static extern int GetFinalPathNameByHandle(SafeFileHandle handle, [In, Out] StringBuilder path, int bufLen, int flags);
		
		public static void SetFileLength(SafeFileHandle fileHandle, long length)
		{
			var lo = (int)(length & 0xffffffff);
			var hi = (int)(length >> 32);

			int lastError;

			if (SetFilePointer(fileHandle, lo, out hi, NativeFileMoveMethod.Begin) == -1)
			{
				lastError = Marshal.GetLastWin32Error();
				if (lastError != 0)
					throw new Win32Exception(lastError);
			}

			if (SetEndOfFile(fileHandle) == false)
			{
				lastError = Marshal.GetLastWin32Error();

				if (lastError == (int) NativeFileErrors.DiskFull)
				{
					var filePath = new StringBuilder(256);

					while (GetFinalPathNameByHandle(fileHandle, filePath, filePath.Capacity, 0) > filePath.Capacity && 
						filePath.Capacity < 32767) // max unicode path length
					{
						filePath = new StringBuilder(filePath.Capacity*2);
					}

					filePath = filePath.Replace(@"\\?\", string.Empty); // remove extended-length path prefix

					var fullFilePath = filePath.ToString();
					var driveLetter = Path.GetPathRoot(fullFilePath);
					var driveInfo = new DriveInfo(driveLetter);

					throw new DiskFullException(driveInfo, fullFilePath, length);
				}

				throw new Win32Exception(lastError);
			}
		}
	}

	public enum NativeFileErrors
	{
		DiskFull = 0x70
	}

	public enum NativeFileMoveMethod : uint
	{
		Begin = 0,
		Current = 1,
		End = 2
	}

	[Flags]
	public enum NativeFileAccess : uint
	{
		//
		// Standard Section
		//

		AccessSystemSecurity = 0x1000000,   // AccessSystemAcl access type
		MaximumAllowed = 0x2000000,     // MaximumAllowed access type

		Delete = 0x10000,
		ReadControl = 0x20000,
		WriteDAC = 0x40000,
		WriteOwner = 0x80000,
		Synchronize = 0x100000,

		StandardRightsRequired = 0xF0000,
		StandardRightsRead = ReadControl,
		StandardRightsWrite = ReadControl,
		StandardRightsExecute = ReadControl,
		StandardRightsAll = 0x1F0000,
		SpecificRightsAll = 0xFFFF,

		FILE_READ_DATA = 0x0001,        // file & pipe
		FILE_LIST_DIRECTORY = 0x0001,       // directory
		FILE_WRITE_DATA = 0x0002,       // file & pipe
		FILE_ADD_FILE = 0x0002,         // directory
		FILE_APPEND_DATA = 0x0004,      // file
		FILE_ADD_SUBDIRECTORY = 0x0004,     // directory
		FILE_CREATE_PIPE_INSTANCE = 0x0004, // named pipe
		FILE_READ_EA = 0x0008,          // file & directory
		FILE_WRITE_EA = 0x0010,         // file & directory
		FILE_EXECUTE = 0x0020,          // file
		FILE_TRAVERSE = 0x0020,         // directory
		FILE_DELETE_CHILD = 0x0040,     // directory
		FILE_READ_ATTRIBUTES = 0x0080,      // all
		FILE_WRITE_ATTRIBUTES = 0x0100,     // all

		//
		// Generic Section
		//

		GenericRead = 0x80000000,
		GenericWrite = 0x40000000,
		GenericExecute = 0x20000000,
		GenericAll = 0x10000000,

		SPECIFIC_RIGHTS_ALL = 0x00FFFF,
		FILE_ALL_ACCESS =
		StandardRightsRequired |
		Synchronize |
		0x1FF,

		FILE_GENERIC_READ =
		StandardRightsRead |
		FILE_READ_DATA |
		FILE_READ_ATTRIBUTES |
		FILE_READ_EA |
		Synchronize,

		FILE_GENERIC_WRITE =
		StandardRightsWrite |
		FILE_WRITE_DATA |
		FILE_WRITE_ATTRIBUTES |
		FILE_WRITE_EA |
		FILE_APPEND_DATA |
		Synchronize,

		FILE_GENERIC_EXECUTE =
		StandardRightsExecute |
		  FILE_READ_ATTRIBUTES |
		  FILE_EXECUTE |
		  Synchronize
	}

	[Flags]
	public enum NativeFileShare : uint
	{
		/// <summary>
		///
		/// </summary>
		None = 0x00000000,
		/// <summary>
		/// Enables subsequent open operations on an object to request read access.
		/// Otherwise, other processes cannot open the object if they request read access.
		/// If this flag is not specified, but the object has been opened for read access, the function fails.
		/// </summary>
		Read = 0x00000001,
		/// <summary>
		/// Enables subsequent open operations on an object to request write access.
		/// Otherwise, other processes cannot open the object if they request write access.
		/// If this flag is not specified, but the object has been opened for write access, the function fails.
		/// </summary>
		Write = 0x00000002,
		/// <summary>
		/// Enables subsequent open operations on an object to request delete access.
		/// Otherwise, other processes cannot open the object if they request delete access.
		/// If this flag is not specified, but the object has been opened for delete access, the function fails.
		/// </summary>
		Delete = 0x00000004
	}

	public enum NativeFileCreationDisposition : uint
	{
		/// <summary>
		/// Creates a new file. The function fails if a specified file exists.
		/// </summary>
		New = 1,
		/// <summary>
		/// Creates a new file, always.
		/// If a file exists, the function overwrites the file, clears the existing attributes, combines the specified file attributes,
		/// and flags with FILE_ATTRIBUTE_ARCHIVE, but does not set the security descriptor that the SECURITY_ATTRIBUTES structure specifies.
		/// </summary>
		CreateAlways = 2,
		/// <summary>
		/// Opens a file. The function fails if the file does not exist.
		/// </summary>
		OpenExisting = 3,
		/// <summary>
		/// Opens a file, always.
		/// If a file does not exist, the function creates a file as if dwCreationDisposition is CREATE_NEW.
		/// </summary>
		OpenAlways = 4,
		/// <summary>
		/// Opens a file and truncates it so that its size is 0 (zero) bytes. The function fails if the file does not exist.
		/// The calling process must open the file with the GENERIC_WRITE access right.
		/// </summary>
		TruncateExisting = 5
	}

	[Flags]
	public enum NativeFileAttributes : uint
	{
		Readonly = 0x00000001,
		Hidden = 0x00000002,
		System = 0x00000004,
		Directory = 0x00000010,
		Archive = 0x00000020,
		Device = 0x00000040,
		Normal = 0x00000080,
		Temporary = 0x00000100,
		SparseFile = 0x00000200,
		ReparsePoint = 0x00000400,
		Compressed = 0x00000800,
		Offline = 0x00001000,
		NotContentIndexed = 0x00002000,
		Encrypted = 0x00004000,
		Write_Through = 0x80000000,
		Overlapped = 0x40000000,
		NoBuffering = 0x20000000,
		RandomAccess = 0x10000000,
		SequentialScan = 0x08000000,
		DeleteOnClose = 0x04000000,
		BackupSemantics = 0x02000000,
		PosixSemantics = 0x01000000,
		OpenReparsePoint = 0x00200000,
		OpenNoRecall = 0x00100000,
		FirstPipeInstance = 0x00080000
	} 
}