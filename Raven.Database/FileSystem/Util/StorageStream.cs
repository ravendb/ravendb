using System;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Exceptions;
using Raven.Database.FileSystem.Storage;
using Raven.Json.Linq;
using Raven.Abstractions.FileSystem;

namespace Raven.Database.FileSystem.Util
{
    public class StorageStream : Stream
    {
        private readonly RavenFileSystem fileSystem;
        private readonly ITransactionalStorage storage;
        private const int PagesBatchSize = 64;
        private long currentOffset;

        private long currentPageFrameOffset;
        private bool disposed;
        private FileAndPagesInformation fileAndPages;
        private FileHeader fileHeader;

        protected byte[] InnerBuffer;
        protected int InnerBufferOffset = 0;
        private int writtingPagePosition;

        protected StorageStream(RavenFileSystem fileSystem, ITransactionalStorage storage, string fileName, RavenJObject metadata, StorageStreamAccess storageStreamAccess)
        {
            this.fileSystem = fileSystem;
            this.storage = storage;

            StorageStreamAccess = storageStreamAccess;
            Name = fileName;

            switch (storageStreamAccess)
            {
                case StorageStreamAccess.Read:
                    storage.Batch(accessor => fileHeader = accessor.ReadFile(fileName));
                    if (fileHeader.TotalSize == null)
                    {
                        throw new FileNotFoundException("File is not uploaded yet");
                    }
                    Metadata = fileHeader.Metadata;
                    Seek(0, SeekOrigin.Begin);
                    break;
                case StorageStreamAccess.CreateAndWrite:

                    if (this.fileSystem == null)
                        throw new ArgumentNullException("fileSystem");

                    storage.Batch(accessor =>
                    {
                        using (fileSystem.DisableAllTriggersForCurrentThread())
                        {
                            fileSystem.Files.IndicateFileToDelete(fileName, null);
                        }

                        var putResult = accessor.PutFile(fileName, null, metadata);
                        fileSystem.Search.Index(fileName, metadata, putResult.Etag);
                    });
                    Metadata = metadata;
                    break;
                default:
                    throw new ArgumentOutOfRangeException("storageStreamAccess", storageStreamAccess, "Unknown value");
            }
        }

        public StorageStreamAccess StorageStreamAccess { get; private set; }
        public string Name { get; private set; }

        public RavenJObject Metadata { get; private set; }

        private long CurrentPageFrameSize
        {
            get { return fileAndPages.Pages.Sum(item => item.Size); }
        }

        public override bool CanRead
        {
            get { return StorageStreamAccess == StorageStreamAccess.Read && fileHeader.TotalSize.HasValue; }
        }

        public override bool CanSeek
        {
            get { return StorageStreamAccess == StorageStreamAccess.Read && fileHeader.TotalSize.HasValue; }
        }

        public override bool CanWrite
        {
            get { return StorageStreamAccess == StorageStreamAccess.CreateAndWrite; }
        }

        public override long Length
        {
            get { return fileHeader.TotalSize ?? 0; }
        }

        public override long Position
        {
            get { return currentOffset; }
            set { Seek(value, SeekOrigin.Begin); }
        }

        public static StorageStream Reading(ITransactionalStorage storage, string fileName)
        {
            return new StorageStream(null, storage, fileName, null, StorageStreamAccess.Read);
        }

        public static StorageStream CreatingNewAndWritting(RavenFileSystem fileSystem, string fileName, RavenJObject metadata)
        {
            return new StorageStream(fileSystem, fileSystem.Storage, fileName, metadata, StorageStreamAccess.CreateAndWrite);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    break;
                case SeekOrigin.Current:
                    offset = currentOffset + offset;
                    break;
                case SeekOrigin.End:
                    offset = Length - offset - 1;
                    break;
                default:
                    throw new ArgumentOutOfRangeException("origin");
            }
            MovePageFrame(offset);
            return currentOffset;
        }

        private void MovePageFrame(long offset)
        {
            offset = Math.Min(Length, offset);
            if (offset < currentPageFrameOffset || fileAndPages == null)
            {
                storage.Batch(accessor => fileAndPages = accessor.GetFile(Name, 0, PagesBatchSize));
                currentPageFrameOffset = 0;
            }
            while (currentPageFrameOffset + CurrentPageFrameSize - 1 < offset)
            {
                var lastPageFrameSize = CurrentPageFrameSize;
                var nextPageIndex = fileAndPages.Start + fileAndPages.Pages.Count;
                storage.Batch(accessor => fileAndPages = accessor.GetFile(Name, nextPageIndex, PagesBatchSize));
                if (fileAndPages.Pages.Count < 1)
                {
                    fileAndPages.Start = 0;
                    break;
                }
                currentPageFrameOffset += lastPageFrameSize;
            }
            currentOffset = offset;
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var innerBuffer = new byte[StorageConstants.MaxPageSize];
            return ReadUsingExternalTempBuffer(buffer, offset, count, innerBuffer);
        }

        public int ReadUsingExternalTempBuffer(byte[] buffer, int offset, int count, byte[] temp)
        {
            if (currentOffset >= Length)
            {
                return 0;
            }
            var pageOffset = currentPageFrameOffset;
            var length = 0L;
            var startingOffset = currentOffset;
            foreach (var page in fileAndPages.Pages)
            {
                if (pageOffset <= currentOffset && currentOffset < pageOffset + page.Size)
                {
                    var pageLength = 0;
                    storage.Batch(accessor => pageLength = accessor.ReadPage(page.Id, temp));
                    var sourceIndex = currentOffset - pageOffset;
                    length = Math.Min(temp.Length - sourceIndex,
                        Math.Min(pageLength, Math.Min(buffer.Length - offset, Math.Min(pageLength - sourceIndex, count))));

                    Array.Copy(temp, sourceIndex, buffer, offset, length);
                    break;
                }
                pageOffset += page.Size;
            }
            MovePageFrame(currentOffset + length);
            return Convert.ToInt32(currentOffset - startingOffset);
        }

        public override void Flush()
        {
            if (InnerBuffer != null && InnerBufferOffset > 0)
            {
                int retries = 50;
                bool shouldRetry;

                do
                {
                    try
                    {
                        storage.Batch(accessor =>
                        {
                            var hashKey = accessor.InsertPage(InnerBuffer, InnerBufferOffset);
                            accessor.AssociatePage(Name, hashKey, writtingPagePosition, InnerBufferOffset);
                            fileSystem.PutTriggers.Apply(trigger => trigger.OnUpload(Name, Metadata, hashKey, writtingPagePosition, InnerBufferOffset));
                        });

                        writtingPagePosition++;

                        shouldRetry = false;
                    }
                    catch (ConcurrencyException)
                    {
                        if (retries-- > 0)
                        {
                            shouldRetry = true;
                            Thread.Sleep(50);
                            continue;
                        }

                        throw;
                    }
                } while (shouldRetry);

                InnerBuffer = null;
                InnerBufferOffset = 0;
            }
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            var innerOffset = 0;

            while (innerOffset < count)
            {
                if (InnerBuffer == null)
                    InnerBuffer = new byte[StorageConstants.MaxPageSize];

                var toCopy = Math.Min(StorageConstants.MaxPageSize - InnerBufferOffset, count - innerOffset);
                if (toCopy == 0)
                    throw new Exception("Impossible");

                Array.Copy(buffer, offset + innerOffset, InnerBuffer, InnerBufferOffset, toCopy);
                InnerBufferOffset += toCopy;

                if (InnerBufferOffset == StorageConstants.MaxPageSize)
                    Flush();

                innerOffset += toCopy;
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (!disposed)
            {
                if (disposing)
                {
                    Flush();

                    if (StorageStreamAccess == StorageStreamAccess.CreateAndWrite)
                    {
                        storage.Batch(accessor => accessor.CompleteFileUpload(Name));
                        fileSystem.PutTriggers.Apply(trigger => trigger.AfterUpload(Name, Metadata));
                    }
                }
                disposed = true;
            }
        }
    }
}
