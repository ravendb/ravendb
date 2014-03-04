// -----------------------------------------------------------------------
//  <copyright file="StorageActionsAccessor.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;

using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Server.RavenFS.Storage.Voron.Impl;
using Raven.Database.Server.RavenFS.Synchronization.Rdc;

using Voron.Impl;

namespace Raven.Database.Server.RavenFS.Storage.Voron
{
    public class StorageActionsAccessor : IStorageActionsAccessor
    {
        private readonly TableStorage tableStorage;

        private readonly Reference<WriteBatch> writeBatchRef;

        private readonly SnapshotReader snapshot;

        private readonly BufferPool bufferPool;

        public StorageActionsAccessor(TableStorage tableStorage, Reference<WriteBatch> writeBatchRef, SnapshotReader snapshot, BufferPool bufferPool)
        {
            this.tableStorage = tableStorage;
            this.writeBatchRef = writeBatchRef;
            this.snapshot = snapshot;
            this.bufferPool = bufferPool;
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public void Commit()
        {
            throw new NotImplementedException();
        }

        public void PulseTransaction()
        {
            throw new NotImplementedException();
        }

        public int InsertPage(byte[] buffer, int size)
        {
            throw new NotImplementedException();
        }

        public void PutFile(string filename, long? totalSize, NameValueCollection metadata, bool tombstone = false)
        {
            throw new NotImplementedException();
        }

        public void AssociatePage(string filename, int pageId, int pagePositionInFile, int pageSize)
        {
            throw new NotImplementedException();
        }

        public int ReadPage(int pageId, byte[] buffer)
        {
            throw new NotImplementedException();
        }

        public FileHeader ReadFile(string filename)
        {
            throw new NotImplementedException();
        }

        public FileAndPages GetFile(string filename, int start, int pagesToLoad)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<FileHeader> ReadFiles(int start, int size)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<FileHeader> GetFilesAfter(Guid etag, int take)
        {
            throw new NotImplementedException();
        }

        public void Delete(string filename)
        {
            throw new NotImplementedException();
        }

        public void UpdateFileMetadata(string filename, NameValueCollection metadata)
        {
            throw new NotImplementedException();
        }

        public void CompleteFileUpload(string filename)
        {
            throw new NotImplementedException();
        }

        public int GetFileCount()
        {
            throw new NotImplementedException();
        }

        public void DecrementFileCount()
        {
            throw new NotImplementedException();
        }

        public void RenameFile(string filename, string rename, bool commitPeriodically = false)
        {
            throw new NotImplementedException();
        }

        public NameValueCollection GetConfig(string name)
        {
            throw new NotImplementedException();
        }

        public void SetConfig(string name, NameValueCollection metadata)
        {
            throw new NotImplementedException();
        }

        public void DeleteConfig(string name)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<SignatureLevels> GetSignatures(string name)
        {
            throw new NotImplementedException();
        }

        public void ClearSignatures(string name)
        {
            throw new NotImplementedException();
        }

        public long GetSignatureSize(int id, int level)
        {
            throw new NotImplementedException();
        }

        public void GetSignatureStream(int id, int level, Action<Stream> action)
        {
            throw new NotImplementedException();
        }

        public void AddSignature(string name, int level, Action<Stream> action)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<string> GetConfigNames(int start, int pageSize)
        {
            throw new NotImplementedException();
        }

        public bool ConfigExists(string name)
        {
            throw new NotImplementedException();
        }

        public IList<NameValueCollection> GetConfigsStartWithPrefix(string prefix, int start, int take)
        {
            throw new NotImplementedException();
        }

        public IList<string> GetConfigNamesStartingWithPrefix(string prefix, int start, int take, out int total)
        {
            throw new NotImplementedException();
        }
    }
}