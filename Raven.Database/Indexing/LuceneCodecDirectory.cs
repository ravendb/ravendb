using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Threading;

using Lucene.Net.Store;

using Microsoft.Win32.SafeHandles;

using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Extensions;
using Raven.Database.Plugins;

using Sparrow;

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
            return OpenInputInner(name);
        }

        private CodecIndexInput OpenInputInner(string name)
        {
            var file = GetFile(name);
            return new CodecIndexInput(file, s => ApplyReadCodecs(file.Name, s));
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
            using (var input = OpenInput(name))
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
        private unsafe class CodecIndexInput : IndexInput
        {
            private readonly FileInfo file;
            private readonly Func<Stream, Stream> applyCodecs;
            private Stream stream;
            private bool isOriginal = true;
            private readonly SafeFileHandle fileHandle;
            private readonly IntPtr mmf;
            private readonly byte* basePtr;
            private readonly CancellationTokenSource cts = new CancellationTokenSource();

            private class MmapStream : Stream
            {
                private readonly string name;
                private readonly byte* ptr;
                private readonly long len;
                private long pos;

                public MmapStream(string name, byte* ptr, long len)
                {
                    this.name = name;
                    this.ptr = ptr;
                    this.len = len;
                }

                public override void Flush()
                {
                }

                public override long Seek(long offset, SeekOrigin origin)
                {
                    switch (origin)
                    {
                        case SeekOrigin.Begin:
                            Position = offset;
                            break;
                        case SeekOrigin.Current:
                            Position += offset;
                            break;
                        case SeekOrigin.End:
                            Position = len + offset;
                            break;
                        default:
                            throw new ArgumentOutOfRangeException("origin", origin, null);
                    }
                    return Position;
                }

                public override void SetLength(long value)
                {
                    throw new NotSupportedException();
                }

                [HandleProcessCorruptedStateExceptions]
                public override int ReadByte()
                {
                    if (Position == len)
                        return -1;
                    try
                    {
                        return ptr[pos++];
                    }
                    catch (AccessViolationException)
                    {
                        throw new ObjectDisposedException("MmapStream", "Cannot access '" + name + "' because the index input has been disposed");
                    }
                }

                [HandleProcessCorruptedStateExceptions]
                public override int Read(byte[] buffer, int offset, int count)
                {
                    if (pos == len)
                        return 0;
                    if (count > len - pos)
                    {
                        count = (int)(len - pos);
                    }
                    try
                    {
                        fixed (byte* dst = buffer)
                        {
                            Memory.CopyInline(dst + offset, ptr + pos, count);
                        }
                    }
                    catch (AccessViolationException)
                    {
                        throw new ObjectDisposedException("MmapStream", "Cannot access '" + name + "' because the index input has been disposed");
                    }
                    pos += count;
                    return count;
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
                    get { return true; }
                }
                public override bool CanWrite
                {
                    get { return false; }
                }
                public override long Length
                {
                    get { return len; }
                }
                public override long Position { get { return pos; } set { pos = value; } }
            }

            public CodecIndexInput(FileInfo file, Func<Stream, Stream> applyCodecs)
            {
                try
                {
                    this.file = file;
                    this.applyCodecs = applyCodecs;
                    if (file.Length == 0)
                    {
                        stream = applyCodecs(Stream.Null);
                        return;
                    }

                    fileHandle = Win32NativeFileMethods.CreateFile(file.FullName,
                        Win32NativeFileAccess.GenericRead,
                        Win32NativeFileShare.Read | Win32NativeFileShare.Write | Win32NativeFileShare.Delete,
                        IntPtr.Zero,
                        Win32NativeFileCreationDisposition.OpenExisting,
                        Win32NativeFileAttributes.RandomAccess,
                        IntPtr.Zero);

                    if (fileHandle.IsInvalid)
                    {
                        const int ERROR_FILE_NOT_FOUND = 2;
                        if (Marshal.GetLastWin32Error() == ERROR_FILE_NOT_FOUND)
                            throw new FileNotFoundException(file.FullName);
                        throw new Win32Exception(Marshal.GetLastWin32Error(), "Could not open file " + file.FullName);
                    }

                    mmf = Win32MemoryMapNativeMethods.CreateFileMapping(fileHandle.DangerousGetHandle(), IntPtr.Zero, Win32MemoryMapNativeMethods.FileMapProtection.PageReadonly,
                        0, 0, null);
                    if (mmf == IntPtr.Zero)
                    {
                        throw new Win32Exception(Marshal.GetLastWin32Error(), "Could not create file mapping for " + file.FullName);
                    }

                    basePtr = Win32MemoryMapNativeMethods.MapViewOfFileEx(mmf,
                        Win32MemoryMapNativeMethods.NativeFileMapAccessType.Read,
                        0, 0, UIntPtr.Zero, null);
                    if (basePtr == null)
                        throw new Win32Exception(Marshal.GetLastWin32Error(), "Could not map file " + file.FullName);

                    stream = applyCodecs(new MmapStream(file.FullName, basePtr, file.Length));
                }
                catch (Exception)
                {
                    Dispose(false);
                    throw;
                }
            }

            public override object Clone()
            {
                if (cts.IsCancellationRequested)
                    throw new ObjectDisposedException("CodecIndexInput");

                var clone = (CodecIndexInput)base.Clone();
                GC.SuppressFinalize(clone);
                clone.isOriginal = false;
                clone.stream = applyCodecs(file.Length != 0 ? new MmapStream(file.Name, basePtr, file.Length) : Stream.Null);
                clone.stream.Position = stream.Position;
                return clone;
            }

            public override byte ReadByte()
            {
                if (cts.IsCancellationRequested)
                    throw new ObjectDisposedException("CodecIndexInput");
                var readByte = stream.ReadByte();
                if (readByte == -1)
                    throw new EndOfStreamException();
                return (byte)readByte;
            }

            public override void ReadBytes(byte[] b, int offset, int len)
            {
                if (cts.IsCancellationRequested)
                    throw new ObjectDisposedException("CodecIndexInput");
                stream.ReadEntireBlock(b, offset, len);
            }

            protected override void Dispose(bool disposing)
            {
                if (stream != null)
                    stream.Dispose();

                GC.SuppressFinalize(this);

                if (isOriginal == false)
                    return;

                cts.Cancel();
                if (basePtr != null)
                    Win32MemoryMapNativeMethods.UnmapViewOfFile(basePtr);
                if (mmf != IntPtr.Zero)
                    Win32NativeMethods.CloseHandle(mmf);
                if (fileHandle != null)
                    fileHandle.Close();
            }

            ~CodecIndexInput()
            {
                try
                {
                    Dispose(false);
                }
                catch (Exception)
                {
                    // nothing can be done here
                }
            }

            public override void Seek(long pos)
            {
                if (cts.IsCancellationRequested)
                    throw new ObjectDisposedException("CodecIndexInput");
                stream.Seek(pos, SeekOrigin.Begin);
            }

            public override long Length()
            {
                return stream.Length;
            }

            public override long FilePointer
            {
                get
                {
                    if (cts.IsCancellationRequested)
                        throw new ObjectDisposedException("CodecIndexInput");
                    return stream.Position;
                }
            }
        }

        private class CodecIndexOutput : BufferedIndexOutput
        {
            private static readonly ILog Log = LogManager.GetCurrentClassLogger();

            private readonly FileInfo file;
            private readonly Stream stream;
            private bool disposed;

            public CodecIndexOutput(FileInfo file, Func<Stream, Stream> applyCodecs)
            {
                this.file = file;
                stream = applyCodecs(file.Open(FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite));
            }

            ~CodecIndexOutput()
            {
                try
                {
                    Dispose(false);
                }
                catch (Exception e)
                {
                    // Can't throw exceptions from the finalizer thread
                    Log.ErrorException("Cannot dispose of CodecIndexOutput: " + e.Message, e);
                }
            }

            public override void FlushBuffer(byte[] b, int offset, int len)
            {
                stream.Write(b, offset, len);
                stream.Flush();
            }

            protected override void Dispose(bool disposing)
            {
                if (disposed)
                    return;

                using (stream)
                {
                base.Dispose(disposing);
            }

                disposed = true;
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
