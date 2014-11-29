// -----------------------------------------------------------------------
//  <copyright file="PosixJournalWriter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Runtime.InteropServices;
using Mono.Unix.Native;
using Voron.Impl;
using Voron.Impl.Journal;
using Voron.Impl.Paging;

namespace Voron.Platform.Posix
{
	/// <summary>
	/// This class assume a single writer and requires external syncronization to work properly
	/// </summary>
	public class PosixJournalWriter : IJournalWriter
	{
		private readonly string _filename;
		private int _fd, _fdReads = -1;

		public PosixJournalWriter(string filename, long journalSize)
		{
			_filename = filename;

			_fd = Syscall.open(filename, OpenFlags.O_WRONLY | OpenFlags.O_SYNC | OpenFlags.O_CREAT,
			                   FilePermissions.ALLPERMS);
			if (_fd == -1)
			{
				PosixHelper.ThrowLastError(Marshal.GetLastWin32Error());
			}

			var result = Syscall.posix_fallocate(_fd, 0, (ulong)journalSize);
			if(result != 0)
				PosixHelper.ThrowLastError(result);

			NumberOfAllocatedPages = journalSize / AbstractPager.PageSize;
		}

		

		public void Dispose()
		{
			Disposed = true;
			GC.SuppressFinalize(this);
			if (_fdReads != -1)
				Syscall.close(_fdReads);
			Syscall.close(_fd);
			if (DeleteOnClose)
			{
				try
				{
					File.Delete(_filename);
				}
				catch (Exception)
				{
					// nothing to do here
				}
			}
		}

		~PosixJournalWriter()
		{
			Dispose();
		}

		public unsafe void WriteGather(long position, byte*[] pages)
		{
			var locs = new Iovec[pages.Length];
			for (int i = 0; i < locs.Length; i++)
			{
				locs[i].iov_base = new IntPtr(pages[i]);
				locs[i].iov_len = AbstractPager.PageSize;
			}
			var result = Syscall.pwritev(_fd, locs, position);
			if(result == -1)
				PosixHelper.ThrowLastError(Marshal.GetLastWin32Error());
		}

		public long NumberOfAllocatedPages { get; private set; }
		public bool Disposed { get; private set; }
		public bool DeleteOnClose { get; set; }

		public IVirtualPager CreatePager()
		{
			return new PosixMemoryMapPager(_filename);
		}

		public unsafe bool Read(long pageNumber, byte* buffer, int count)
		{
			if (_fdReads == -1)
			{
				_fdReads = Syscall.open(_filename, OpenFlags.O_RDONLY);
				if (_fdReads == -1)
					PosixHelper.ThrowLastError(Marshal.GetLastWin32Error());
			}
			long position = pageNumber * AbstractPager.PageSize;

			while (count > 0)
			{
				var result = Syscall.pread(_fdReads, buffer, (ulong)count, position);
				if (result == 0) //eof
					return false;
				count -= (int) result;
				buffer += result;
				position += result;
			}
			return true;
		}
	}
}