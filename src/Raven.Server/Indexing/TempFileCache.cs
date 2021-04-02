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
        private readonly ConcurrentQueue<FileStream> _files = new ConcurrentQueue<FileStream>();
        private readonly ConcurrentQueue<MemoryStream> _ms = new ConcurrentQueue<MemoryStream>();

        private const long MaxFileSizeToReduceInBytes = 16 * Constants.Size.Megabyte;
        private const int MaxFilesToKeepInCache = 32;

        public TempFileCache(StorageEnvironmentOptions options)
        {
            _options = options;
            string path = _options.TempPath.FullPath;
            if (Directory.Exists(path) == false)
                Directory.CreateDirectory(path);
            foreach (string file in Directory.GetDirectories(path,  "lucene-*" + StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions.TempFileExtension))
            {
                var info = new FileInfo(file);
                if (info.Length > MaxFileSizeToReduceInBytes || 
                    _files.Count >= MaxFilesToKeepInCache)
                {
                    File.Delete(file);
                }
                else
                {
                    FileStream fileStream;
                    try
                    {
                        fileStream = new FileStream(file, FileMode.Open);
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

        public string FullPath  => _options.TempPath.FullPath;

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
                stream = SafeFileStream.Create(
                    GetTempFileName(_options),
                    FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read, 4096,
                    FileOptions.DeleteOnClose);
            }
            Stream resultStream = stream;
            if (_options.Encryption.IsEnabled)
                resultStream= new TempCryptoStream(stream).IgnoreSetLength();
            return resultStream;
        }

        public static string GetTempFileName(StorageEnvironmentOptions options)
        {
            return Path.Combine(options.TempPath.FullPath, "lucene-" + Guid.NewGuid() +  StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions.TempFileExtension);
        }

        public void ReturnFileStream(Stream stream)
        {
            FileStream s = stream switch
            {
                TempCryptoStream tcs => tcs.InnerStream,
                FileStream fs => fs,
                _ => throw new ArgumentOutOfRangeException()
            };

            if (s.Length > MaxFileSizeToReduceInBytes || 
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
            foreach (FileStream file in _files)
            {
                DisposeFile(file);
            }
        }

        private static void DisposeFile(FileStream file)
        {
            string fileName = null;
            try
            {
                fileName = file.Name;
                file.Dispose();
            }
            catch (Exception e)
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
}
