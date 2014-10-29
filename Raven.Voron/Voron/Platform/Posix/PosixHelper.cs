// -----------------------------------------------------------------------
//  <copyright file="NativeMethods.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using Mono.Unix.Native;
using Voron.Impl.FileHeaders;
using Voron.Platform.Win32;

namespace Voron.Platform.Posix
{
	internal class PosixHelper
	{
		public static void ThrowLastError(int lastError)
		{
			if (Enum.IsDefined(typeof(Errno), lastError) == false)
				throw new InvalidOperationException("Unknown errror " + lastError);
			var error = (Errno)lastError;
			throw new InvalidOperationException(error.ToString());
		}

		public static unsafe void WriteFileHeader(FileHeader* header, string path)
		{
			var fd = Syscall.open(path, OpenFlags.O_WRONLY | OpenFlags.O_CREAT,
			                      FilePermissions.ALLPERMS);
			try
			{
				if (fd == -1)
					ThrowLastError(Marshal.GetLastWin32Error());
				int remaining = sizeof(FileHeader);
				var ptr = ((byte*)header);
				while (remaining > 0)
				{
					var written = Syscall.write(fd, ptr, (ulong)remaining);
					if (written == -1)
						ThrowLastError(Marshal.GetLastWin32Error());

					remaining -= (int) written;
					ptr += written;
				}
				Syscall.fsync(fd);
			}
			finally
			{
				if (fd != -1)
					Syscall.close(fd);
			}
		}

		public static unsafe bool TryReadFileHeader(FileHeader* header, string path)
		{
			var fd = Syscall.open(path, OpenFlags.O_RDONLY);
			try
			{
				if (fd == -1)
				{
					var lastError = Marshal.GetLastWin32Error();
					if (((Errno) lastError) == Errno.EACCES)
						return false;
					ThrowLastError(lastError);
				}
				int remaining = sizeof(FileHeader);
				var ptr = ((byte*)header);
				while (remaining > 0)
				{
					var read = Syscall.read(fd, ptr, (ulong)remaining);
					if (read == -1)
						ThrowLastError(Marshal.GetLastWin32Error());

					if (read == 0)
						return false;// truncated file?

					remaining -= (int)read;
					ptr += read;
				}
				return true;
			}
			finally
			{
				if (fd != -1)
					Syscall.close(fd);
			}
		}
	}

	internal static class Rt
	{
		[DllImport("rt", SetLastError = true)]
		public extern static int shm_open (string name, OpenFlags oflag, int mode);

		[DllImport("rt", SetLastError = true)]
		public extern static int shm_unlink (string name);

	}
}