using System;
using System.Collections.Concurrent;
using System.IO;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
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
        private readonly ConcurrentQueue<MemoryStream> _ms = new ConcurrentQueue<MemoryStream>();

        private const long MaxFileSizeToKeepInBytes = 16 * Constants.Size.Megabyte;
        private const int MaxFilesToKeepInCache = 32;

        public TempFileCache(StorageEnvironmentOptions options)
        {
            _options = options;
            string path = _options.TempPath.FullPath;
            if (Directory.Exists(path) == false)
                Directory.CreateDirectory(path);
            foreach (string file in Directory.GetDirectories(path, "lucene-*" + StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions.TempFileExtension))
            {
                var info = new FileInfo(file);
                if (info.Length > MaxFileSizeToKeepInBytes ||
                    _files.Count >= MaxFilesToKeepInCache)
                {
                    File.Delete(file);
                }
                else
                {
                    TempFileStream fileStream;
                    try
                    {
                        fileStream = new TempFileStream(new FileStream(file, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, 4096, FileOptions.DeleteOnClose));
                        fileStream.ResetLength();
                    }
                    catch (IOException)
                    {
                        // if can't open, just ignore it.
                        continue;
                    }
                    _files.Enqueue(fileStream);
                }
            }

            LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
        }

        public string FullPath => _options.TempPath.FullPath;

        public MemoryStream RentMemoryStream()
        {
            return _ms.TryDequeue(out var ms) ? ms : new MemoryStream(128 * Constants.Size.Kilobyte);
        }

        public void ReturnMemoryStream(MemoryStream stream)
        {
            stream.SetLength(0);
            _ms.Enqueue(stream);
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
                stream.ResetLength();

            Stream resultStream = stream;
            if (_options.Encryption.IsEnabled)
                resultStream = new TempCryptoStream(stream).IgnoreSetLength();
            return resultStream;
        }

        public static string GetTempFileName(StorageEnvironmentOptions options)
        {
            return Path.Combine(options.TempPath.FullPath, "lucene-" + Guid.NewGuid() + StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions.TempFileExtension);
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
            foreach (var file in _files)
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

            while (_ms.TryDequeue(out var ms))
            {
                ms.Dispose();
            }
        }

        public void LowMemoryOver()
        {
        }
    }

    public class TempFileStream : Stream
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
            return InnerStream.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            InnerStream.SetLength(value);
            _length = value;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            InnerStream.Write(buffer, offset, count);
            _length += count;
        }

        public override bool CanRead => InnerStream.CanRead;
        public override bool CanSeek => InnerStream.CanSeek;
        public override bool CanWrite => InnerStream.CanWrite;
        public override long Length => _length;
        public override long Position { get => InnerStream.Position; set => InnerStream.Position = value; }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            InnerStream.Dispose();
        }

        public void ResetLength()
        {
            _length = 0;
        }
    }
}
