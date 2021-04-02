using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Global;
using Sparrow.Utils;
using Voron;
using Voron.Platform.Posix;

namespace Raven.Server.Indexing
{
    /// <summary>
    /// This assume single threaded and is meant to be used for indexing only
    /// </summary>
    public sealed class TempFileCache : IDisposable
    {
        private readonly StorageEnvironmentOptions _options;
        private readonly Queue<FileStream> _files = new Queue<FileStream>();
        private readonly Queue<MemoryStream> _ms = new Queue<MemoryStream>();

        private const long MaxFileSizeToReduce = 16 * Constants.Size.Megabyte;
        private const int MaxFilesInCache = 32;

        public TempFileCache(StorageEnvironmentOptions options)
        {
            _options = options;
            string path = _options.TempPath.FullPath;
            if (Directory.Exists(path) == false)
                Directory.CreateDirectory(path);
            foreach (string file in Directory.GetDirectories(path,  "*" + StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions.TempFileExtension))
            {
                var info = new FileInfo(file);
                if (info.Length > MaxFileSizeToReduce || 
                    _files.Count >= MaxFilesInCache)
                {
                    File.Delete(file);
                }
                else
                {
                    _files.Enqueue(new FileStream(file, FileMode.OpenOrCreate));
                }
            }
        }

        public string FullPath  => _options.TempPath.FullPath;

        public MemoryStream RentMemoryStream()
        {
            return _ms.Count > 0 ? _ms.Dequeue() : new MemoryStream(128 * Constants.Size.Kilobyte);
        }

        public void ReturnMemoryStream(MemoryStream stream)
        {
            stream.SetLength(0);
            _ms.Enqueue(stream);
        }

        public Stream RentFileStream()
        {
            FileStream stream;
            if (_files.Count > 0)
            {
                stream = _files.Dequeue();
            }
            else
            {
                stream = SafeFileStream.Create(
                    Path.Combine(_options.TempPath.FullPath, Guid.NewGuid() +  StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions.TempFileExtension),
                    FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read, 4096,
                    FileOptions.DeleteOnClose);
            }
            Stream resultStream = stream;
            if (_options.Encryption.IsEnabled)
                resultStream= new TempCryptoStream(stream).IgnoreSetLength();
            return resultStream;
        }

        public void ReturnFileStream(Stream stream)
        {
            FileStream s = stream switch
            {
                TempCryptoStream tcs => tcs.InnerStream,
                FileStream fs => fs,
                _ => throw new ArgumentOutOfRangeException()
            };

            if (s.Length > MaxFileSizeToReduce || 
                _files.Count >= MaxFilesInCache)
            {
                var fileName = s.Name;
                s.Dispose();
                PosixFile.DeleteOnClose(fileName);
                return;
            }

            s.Position = 0;
            _files.Enqueue(s);
        }
        
        public void Dispose()
        {
            foreach (FileStream file in _files)
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
                    if(file != null)
                        PosixFile.DeleteOnClose(fileName);

                }
            }
        }
    }
}
