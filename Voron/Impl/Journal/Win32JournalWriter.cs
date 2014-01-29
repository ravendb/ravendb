using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron.Impl.Journal
{
	/// <summary>
	/// This class assumes only a single writer at any given point in time
	/// This require _external_ syncornization
	/// </summary>
	public unsafe class Win32FileJournalWriter : IJournalWriter
	{
		private const int ErrorIOPending = 997;
		private const int ErrorSuccess = 0;
		private const int ErrorHandleEof = 38;
		private readonly string _filename;
		private readonly SafeFileHandle _handle;
		private readonly ManualResetEvent _manualResetEvent;
		private SafeFileHandle _readHandle;
		private FileSegmentElement* _segments;
		private int _segmentsSize;
		private NativeOverlapped* _nativeOverlapped;

		public Win32FileJournalWriter(string filename, long journalSize)
		{
			_filename = filename;
			_handle = NativeFileMethods.CreateFile(filename,
				NativeFileAccess.GenericWrite, NativeFileShare.Read, IntPtr.Zero,
				NativeFileCreationDisposition.OpenAlways,
				NativeFileAttributes.Write_Through | NativeFileAttributes.NoBuffering | NativeFileAttributes.Overlapped, IntPtr.Zero);

			if (_handle.IsInvalid)
				throw new Win32Exception();

			NativeFileMethods.SetFileLength(_handle, journalSize);

			NumberOfAllocatedPages = journalSize/AbstractPager.PageSize;
			_manualResetEvent = new ManualResetEvent(false);

			_nativeOverlapped = (NativeOverlapped*) Marshal.AllocHGlobal(sizeof (NativeOverlapped));

			_nativeOverlapped->InternalLow = IntPtr.Zero;
			_nativeOverlapped->InternalHigh = IntPtr.Zero;

		}

		public void WriteGather(long position, byte*[] pages)
		{
			if (Disposed)
				throw new ObjectDisposedException("Win32JournalWriter");

			_manualResetEvent.Reset();

			EnsureSegmentsSize(pages);

		
			_nativeOverlapped->OffsetLow = (int) (position & 0xffffffff);
			_nativeOverlapped->OffsetHigh = (int) (position >> 32);
			_nativeOverlapped->EventHandle = _manualResetEvent.SafeWaitHandle.DangerousGetHandle();

			for (int i = 0; i < pages.Length; i++)
			{
				if(IntPtr.Size == 4)
					_segments[i].Alignment = (ulong) pages[i];
				else
					_segments[i].Buffer = pages[i];
			}
			_segments[pages.Length].Buffer = null; // null terminating

			WriteFileGather(_handle, _segments, (uint) pages.Length*4096, IntPtr.Zero, _nativeOverlapped);
			switch (Marshal.GetLastWin32Error())
			{
				case ErrorSuccess:
				case ErrorIOPending:
					_manualResetEvent.WaitOne();
					break;
				default:
					throw new Win32Exception(Marshal.GetLastWin32Error());
			}
		}

		private void EnsureSegmentsSize(byte*[] pages)
		{
			if (_segmentsSize >= pages.Length + 1)
				return;

			_segmentsSize = (int) Utils.NearestPowerOfTwo(pages.Length + 1);

			if (_segments != null)
				Marshal.FreeHGlobal((IntPtr) _segments);

			_segments = (FileSegmentElement*) (Marshal.AllocHGlobal(_segmentsSize*sizeof (FileSegmentElement)));
		}

		public long NumberOfAllocatedPages { get; private set; }
		public bool DeleteOnClose { get; set; }

		public IVirtualPager CreatePager()
		{
			return new Win32MemoryMapPager(_filename);
		}

		public bool Read(long pageNumber, byte* buffer, int count)
		{
			if (_readHandle == null)
			{
				_readHandle = NativeFileMethods.CreateFile(_filename,
					NativeFileAccess.GenericRead,
					NativeFileShare.Write | NativeFileShare.Read | NativeFileShare.Delete,
					IntPtr.Zero,
					NativeFileCreationDisposition.OpenExisting,
					NativeFileAttributes.Normal,
					IntPtr.Zero);
			}

			long position = pageNumber*AbstractPager.PageSize;
			var overlapped = new Overlapped((int) (position & 0xffffffff), (int) (position >> 32), IntPtr.Zero, null);
			NativeOverlapped* nativeOverlapped = overlapped.Pack(null, null);
			try
			{
				while (count > 0)
				{
					int read;
					if (NativeFileMethods.ReadFile(_readHandle, buffer, count, out read, nativeOverlapped) == false)
					{
						int lastWin32Error = Marshal.GetLastWin32Error();
						if (lastWin32Error == ErrorHandleEof)
							return false;
						throw new Win32Exception(lastWin32Error);
					}
					count -= read;
					buffer += read;
				}
				return true;
			}
			finally
			{
				Overlapped.Free(nativeOverlapped);
			}
		}

		public void Dispose()
		{
			Disposed = true;
			GC.SuppressFinalize(this);
			if (_readHandle != null)
				_readHandle.Close();
			_handle.Close();
			if (_nativeOverlapped != null)
			{
				Marshal.FreeHGlobal((IntPtr) _nativeOverlapped);
				_nativeOverlapped = null;
			}
			if (_segments != null)
			{
				Marshal.FreeHGlobal((IntPtr) _segments);
				_segments = null;
			}

			if(_manualResetEvent != null)
				_manualResetEvent.Dispose();

			if (DeleteOnClose)
			{
				File.Delete(_filename);
			}
		}

		public bool Disposed { get; private set; }

		[DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAsAttribute(UnmanagedType.Bool)]
		private static extern bool WriteFileGather(
			SafeFileHandle hFile,
			FileSegmentElement* aSegmentArray,
			uint nNumberOfBytesToWrite,
			IntPtr lpReserved,
			NativeOverlapped* lpOverlapped);

		~Win32FileJournalWriter()
		{
			Dispose();
		}

		[StructLayout(LayoutKind.Explicit, Size = 8)]
		public struct FileSegmentElement
		{
			[FieldOffset(0)] public byte* Buffer;
			[FieldOffset(0)] public UInt64 Alignment;
		}
	}
}
