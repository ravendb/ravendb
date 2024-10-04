using System;
using System.Collections.Concurrent;
using System.IO;
using Microsoft.IO;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Global;
using Sparrow.LowMemory;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Indexing
{
    /// <summary>
    /// This assume single threaded and is meant to be used for indexing only
    /// We need to handle multi threaded from the low memory notifications, though 
    /// </summary>
    public sealed class TempFileCache : IDisposable, ILowMemoryHandler
    {
        private readonly StorageEnvironmentOptions _options;
        private readonly ConcurrentQueue<TempFileStream> _files = new ConcurrentQueue<TempFileStream>();

        private const long MaxFileSizeToKeepInBytes = 16 * Constants.Size.Megabyte;
        internal const int MaxFilesToKeepInCache = 32;
        private int _memoryStreamCapacity = 128 * Constants.Size.Kilobyte;
        internal const string FilePrefix = "lucene-";

        public int FilesCount => _files.Count;

        public TempFileCache(StorageEnvironmentOptions options)
        {
            _options = options;
            string path = _options.TempPath.FullPath;
            if (Directory.Exists(path) == false)
                Directory.CreateDirectory(path);

            foreach (string file in Directory.GetFiles(path, $"{FilePrefix}*" + StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions.TempFileExtension))
            {
                var info = new FileInfo(file);
                if (info.Length > MaxFileSizeToKeepInBytes || _files.Count >= MaxFilesToKeepInCache)
                {
                    IOExtensions.DeleteFile(file);
                }
                else
                {
                    try
                    {
                        var stream = new FileStream(file, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, 4096, FileOptions.DeleteOnClose);
                        var fileStream = new TempFileStream(stream);
                        fileStream.ResetLength();
                        _files.Enqueue(fileStream);
                    }
                    catch (IOException)
                    {
                        // if can't open, just ignore it.
                    }
                }
            }

            LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
        }

        public string FullPath => _options.TempPath.FullPath;

        public void SetMemoryStreamCapacity(int capacity)
        {
            _memoryStreamCapacity = capacity;
        }

        public RecyclableMemoryStream RentMemoryStream()
        {
            return RecyclableMemoryStreamFactory.GetRecyclableStream(_memoryStreamCapacity);
        }

        public void ReturnMemoryStream(RecyclableMemoryStream stream)
        {
            stream.Dispose();
        }

        public Stream RentFileStream()
        {
            if (_files.TryDequeue(out var stream) == false)
            {
                stream = new TempFileStream(SafeFileStream.Create(
                    GetTempFileName(_options),
                    FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read, 4096,
                    FileOptions.DeleteOnClose));
            }
            else
            {
                stream.ResetLength();
            }

            Stream resultStream = stream;
            if (_options.Encryption.IsEnabled)
                resultStream = new TempCryptoStream(stream).IgnoreSetLength();
            return resultStream;
        }

        public static string GetTempFileName(StorageEnvironmentOptions options)
        {
            return Path.Combine(options.TempPath.FullPath, FilePrefix + Guid.NewGuid() + StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions.TempFileExtension);
        }

        public void ReturnFileStream(Stream stream)
        {
            TempFileStream s = stream switch
            {
                TempCryptoStream tcs => (TempFileStream)tcs.InnerStream,
                TempFileStream bs => bs,
                _ => throw new ArgumentOutOfRangeException()
            };

            if (s.InnerStream.Length > MaxFileSizeToKeepInBytes ||
                _files.Count >= MaxFilesToKeepInCache)
            {
                DisposeFile(s);
                return;
            }

            s.Position = 0;
            _files.Enqueue(s);
        }

        public void Dispose()
        {
            while (_files.TryDequeue(out var file))
            {
                DisposeFile(file);
            }
        }

        private static void DisposeFile(TempFileStream file)
        {
            string fileName = null;
            try
            {
                fileName = file.InnerStream.Name;
                file.Dispose();
            }
            catch (Exception)
            {
                // no big deal
            }
            finally
            {
                if (file != null)
                    PosixFile.DeleteOnClose(fileName);
            }
        }

        public void LowMemory(LowMemorySeverity lowMemorySeverity)
        {
            while (_files.TryDequeue(out var s))
            {
                DisposeFile(s);
            }
        }

        public void LowMemoryOver()
        {
        }
    }

    public sealed class TempFileStream : Stream
    {
        public FileStream InnerStream;
        private long _length;

        public TempFileStream(FileStream inner)
        {
            InnerStream = inner ?? throw new ArgumentNullException(nameof(inner));
            _length = inner.Length;
        }

        public override void Flush()
        {
            InnerStream.Flush();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var toRead = (int)Math.Min(count, _length - InnerStream.Position);
            return InnerStream.Read(buffer, offset, toRead);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            long seek = InnerStream.Seek(offset, origin);
            if (seek > _length)
                throw new ArgumentOutOfRangeException($"Cannot seek ({seek}) beyond the end of the file ({_length})");
            return seek;
        }

        public override void SetLength(long value)
        {
            try
            {
                if (InnerStream.Length < value)
                    InnerStream.SetLength(value);

                _length = value;
            }
            catch (IOException e) when (e.IsOutOfDiskSpaceException())
            {
                ExceptionHelper.ThrowDiskFullException(InnerStream.Name);
            }
            catch (IOException e) when (e.IsMediaWriteProtected())
            {
                ExceptionHelper.ThrowMediaIsWriteProtected(e);
            }
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            var pos = InnerStream.Position;
            try
            {
                InnerStream.Write(buffer, offset, count);
                _length = Math.Max(_length, pos + count);
            }
            catch (IOException e) when(e.IsOutOfDiskSpaceException())
            {
                ExceptionHelper.ThrowDiskFullException(InnerStream.Name);
            }
            catch (IOException e) when (e.IsMediaWriteProtected())
            {
                ExceptionHelper.ThrowMediaIsWriteProtected(e);
            }
        }

        public override bool CanRead => InnerStream.CanRead;
        public override bool CanSeek => InnerStream.CanSeek;
        public override bool CanWrite => InnerStream.CanWrite;
        public override long Length => _length;

        public override long Position
        {
            get => InnerStream.Position;
            set
            {
                if (value > _length)
                    throw new ArgumentOutOfRangeException($"Cannot set position ({value}) beyond the end of the file ({_length})");
                InnerStream.Position = value;
            }
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            InnerStream.Dispose();
        }

        public void ResetLength()
        {
            _length = 0;
            InnerStream.Position = 0;
        }
    }
}
