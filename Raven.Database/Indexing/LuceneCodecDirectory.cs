using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using Lucene.Net.Store;
using Microsoft.Win32.SafeHandles;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Voron.Platform.Win32;

namespace Raven.Database.Indexing
{
	public class LuceneCodecDirectory : FSDirectory
	{
		private readonly List<AbstractIndexCodec> codecs;

		public LuceneCodecDirectory(string path, IEnumerable<AbstractIndexCodec> codecs)
			: base(new DirectoryInfo(path), null)
		{
			this.codecs = (codecs ?? Enumerable.Empty<AbstractIndexCodec>()).ToList();
		}

		public override IndexInput OpenInput(string name, int bufferSize)
		{
			return OpenInputInner(name, bufferSize);
		}

		private CodecIndexInput OpenInputInner(string name, int bufferSize)
		{
			var file = GetFile(name);
			return new CodecIndexInput(file, s => ApplyReadCodecs(file.Name, s), bufferSize);
		}

		public override IndexOutput CreateOutput(string name)
		{
			var file = GetFile(name);

			CreateDirectory();
			DeleteFile(file);

			return new CodecIndexOutput(file, s => ApplyWriteCodecs(file.Name, s));
		}

		public override long FileLength(string name)
		{
			using (var input = OpenInputInner(name, bufferSize: BufferedIndexInput.BUFFER_SIZE))
				return input.Length();
		}

		private Stream ApplyReadCodecs(string key, Stream stream)
		{
			try
			{
				foreach (var codec in codecs)
					stream = codec.Decode(key, stream);
				return stream;
			}
			catch
			{
				stream.Dispose();
				throw;
			}
		}

		private Stream ApplyWriteCodecs(string key, Stream stream)
		{
			try
			{
				foreach (var codec in codecs)
					stream = codec.Encode(key, stream);
				return stream;
			}
			catch
			{
				stream.Dispose();
				throw;
			}
		}

		private FileInfo GetFile(string name)
		{
			return new FileInfo(Path.Combine(Directory.FullName, name));
		}

		private void CreateDirectory()
		{
			if (!Directory.Exists)
			{
				Directory.Create();
				Directory.Refresh();
			}
		}

		private void DeleteFile(FileInfo file)
		{
			if (file.Exists)
			{
				try
				{
					IOExtensions.DeleteFile(file.FullName);
				}
				catch (Exception e)
				{
					throw new IOException("Cannot overwrite " + file, e);
				}
			}
		}

		public class ConcurrentReadOnlyWin32FileStream : Stream
		{
			private readonly SafeFileHandle handle;
			[ThreadStatic] public static long NextPosition;

			public ConcurrentReadOnlyWin32FileStream(SafeFileHandle handle)
			{
				this.handle = handle;
			}

			public unsafe override int Read(byte[] buffer, int offset, int count)
			{
				var overlapped = new Overlapped(
					(int) (NextPosition & 0xffffffff),
					(int) (NextPosition >> 32),
					IntPtr.Zero,
					null
					);

				var nativeOverlapped = overlapped.Pack(null,null);
				try
				{
					fixed (byte* p = buffer)
					{
						int read;
						if (Win32NativeFileMethods.ReadFile(handle, p + offset, count, out read, nativeOverlapped) == false)
						{
							const int ERROR_IO_PENDING = 997;

							if (Marshal.GetLastWin32Error() != ERROR_IO_PENDING)
								throw new Win32Exception();
						}

						uint transferred;
						if(Win32NativeFileMethods.GetOverlappedResult(handle, nativeOverlapped, out transferred, true) == false)
							throw new Win32Exception();

						return (int)transferred;
					}
				}
				finally
				{
					Overlapped.Free(nativeOverlapped);
				}
			}
			public override void Flush()
			{
				throw new NotSupportedException();
			}

			public override long Seek(long offset, SeekOrigin origin)
			{
				throw new NotSupportedException();
			}

			public override void SetLength(long value)
			{
				throw new NotSupportedException();
			}

		

			public override void Write(byte[] buffer, int offset, int count)
			{
				throw new NotSupportedException();
			}

			public override bool CanRead
			{
				get { return true; }
			}
			public override bool CanSeek
			{
				get { return false; }
			}
			public override bool CanWrite
			{
				get { return false; }
			}
			public override long Length
			{
				get { throw new NotSupportedException(); }
				
			}
			public override long Position
			{
				get { throw new NotSupportedException(); }
				set { throw new NotSupportedException(); }
			}
		}

		private class CodecIndexInput : BufferedIndexInput
		{
			private FileInfo file;
			private Func<Stream, Stream> applyCodecs;
			private Stream stream;
			private SafeFileHandle fileHandle;
			private int usageCount = 1;

			public CodecIndexInput(FileInfo file, Func<Stream, Stream> applyCodecs, int bufferSize)
				: base(bufferSize)
			{
				this.file = file;
				this.applyCodecs = applyCodecs;

				fileHandle = Win32NativeFileMethods.CreateFile(file.FullName,
					Win32NativeFileAccess.GenericRead,
					Win32NativeFileShare.Read | Win32NativeFileShare.Write,
					IntPtr.Zero,
					Win32NativeFileCreationDisposition.OpenExisting,
					Win32NativeFileAttributes.Overlapped,
					IntPtr.Zero);

				if (fileHandle.IsInvalid)
				{
					const int ERROR_FILE_NOT_FOUND = 2;
					if (Marshal.GetLastWin32Error() == ERROR_FILE_NOT_FOUND)
						throw new FileNotFoundException(file.FullName);
					throw new Win32Exception();
				}

				this.stream = applyCodecs(new ConcurrentReadOnlyWin32FileStream(fileHandle));
			}

			public override long Length()
			{
				return file.Length;
			}

			protected override void Dispose(bool disposing)
			{
				stream.Close();
				if (Interlocked.Decrement(ref usageCount) == 0)
					fileHandle.Close();
			}

			public override void ReadInternal(byte[] b, int offset, int length)
			{
				ConcurrentReadOnlyWin32FileStream.NextPosition = FilePointer;

				stream.ReadEntireBlock(b, offset, length);
			}

			public override void SeekInternal(long pos)
			{
			}

			public override object Clone()
			{
				Interlocked.Increment(ref usageCount);
				var codecIndexInput = (CodecIndexInput) base.Clone();
				codecIndexInput.file = file;
				codecIndexInput.applyCodecs = applyCodecs;
				codecIndexInput.stream = applyCodecs(new ConcurrentReadOnlyWin32FileStream(fileHandle));
				return codecIndexInput;
			}
		}

		private class CodecIndexOutput : BufferedIndexOutput
		{
			static readonly ILog log = LogManager.GetCurrentClassLogger();

			private readonly FileInfo file;
			private readonly Stream stream;

			public CodecIndexOutput(FileInfo file, Func<Stream, Stream> applyCodecs)
			{
				this.file = file;
				stream = applyCodecs(file.Open(FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite));
			}

			~CodecIndexOutput()
			{
				try
				{
					log.Error("~CodecIndexOutput() " + file.FullName + "!");
					Dispose(false);
				}
				catch (Exception e)
				{
					// Can't throw exceptions from the finalizer thread
					log.ErrorException("Cannot dispose of CodecIndexOutput: " + e.Message, e);
				}
			}

			public override void FlushBuffer(byte[] b, int offset, int len)
			{
				stream.Write(b, offset, len);
				stream.Flush();
			}

			protected override void Dispose(bool disposing)
			{
				base.Dispose(disposing);
			    if (stream != null)
			        stream.Close();
				GC.SuppressFinalize(this);
			}

			public override void Seek(long pos)
			{
				base.Seek(pos);
				stream.Seek(pos, SeekOrigin.Begin);
			}

			public override void SetLength(long length)
			{
				stream.SetLength(length);
			}

			public override long Length
			{
				get { return stream.Length; }
			}
		}
	}
}
