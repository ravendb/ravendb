// -----------------------------------------------------------------------
//  <copyright file="StorageActionsAccessor.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Text;
using System.Web;

using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Storage.Voron.Impl;
using Raven.Database.Server.RavenFS.Synchronization.Rdc;
using Raven.Database.Server.RavenFS.Util;
using Raven.Json.Linq;

using Voron;
using Voron.Impl;

namespace Raven.Database.Server.RavenFS.Storage.Voron
{
    public class StorageActionsAccessor : StorageActionsBase, IStorageActionsAccessor
    {
        private readonly TableStorage storage;

        private readonly Reference<WriteBatch> writeBatch;

        public StorageActionsAccessor(TableStorage storage, Reference<WriteBatch> writeBatch, SnapshotReader snapshot, BufferPool bufferPool)
            : base(snapshot, bufferPool)
        {
            this.storage = storage;
            this.writeBatch = writeBatch;
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
            storage.Write(writeBatch.Value);
            writeBatch.Value.Dispose();
            writeBatch.Value = new WriteBatch { DisposeAfterWrite = writeBatch.Value.DisposeAfterWrite };
        }

        public int InsertPage(byte[] buffer, int size)
        {
            var hashKey = new HashKey(buffer, size);
            var key = ConvertToKey(hashKey);

            var pageByKey = storage.Pages.GetIndex(Tables.Pages.Indices.ByKey);

            var result = pageByKey.Read(Snapshot, key, writeBatch.Value);
            if (result != null)
            {
                var id = result.Reader.ToStringValue();

                ushort version;
                var page = LoadJson(storage.Pages, id, writeBatch.Value, out version);
                if (page == null)
                    throw new InvalidOperationException(string.Format("Could not find page '{0}'. Probably data is corrupted.", id));

                var usageCount = page.Value<int>("usage_count");
                page["usage_count"] = usageCount + 1;

                storage.Pages.Add(writeBatch.Value, key, page, version);

                return page.Value<int>("id");
            }

            var newId = -1; // TODO
            var newKey = CreateKey(newId);

            var newPage = new RavenJObject
                   {
                       {"id", newId},
                       {"page_strong_hash", hashKey.Strong},
                       {"page_weak_hash", hashKey.Weak},
                       {"data", buffer},
                       {"usage_count", 0}
                   };

            storage.Pages.Add(writeBatch.Value, newKey, newPage, 0);
            pageByKey.Add(writeBatch.Value, key, newKey);

            return newId;
        }

        public void PutFile(string filename, long? totalSize, NameValueCollection metadata, bool tombstone = false)
        {
            var fileNameHash = HashKey(filename);
            var key = CreateKey(filename, fileNameHash);

            if (!metadata.AllKeys.Contains("ETag"))
					throw new InvalidOperationException(string.Format("Metadata of file {0} does not contain 'ETag' key", filename));

            var innerMetadata = new NameValueCollection(metadata);
            var etag = innerMetadata.Value<Guid>("ETag");
            innerMetadata.Remove("ETag");

            var file = new RavenJObject
                       {
                           {"name", filename},
                           {"total_size", totalSize ?? 0},
                           {"uploaded_size", 0},
                           {"etag", etag.ToString()},
                           {"metadata", ToQueryString(innerMetadata)}
                       };

            storage.Files.Add(writeBatch.Value, key, file, 0);

            if (tombstone)
                return;

            // TODO this code maybe needs to be removed. Need to check for what we are using 'Details'

            ushort version;
            var details = LoadJson(storage.Details, Tables.Details.Key, writeBatch.Value, out version);

            if (details == null)
                throw new InvalidOperationException("Could not find system metadata row");

            var fileCount = details.Value<int>("file_count");

            details["file_count"] = fileCount + 1;

            storage.Details.Add(writeBatch.Value, Tables.Details.Key, details, version);
        }

        public void AssociatePage(string filename, int pageId, int pagePositionInFile, int pageSize)
        {
            throw new NotImplementedException();
        }

        public int ReadPage(int pageId, byte[] buffer)
        {
            var key = CreateKey(pageId);

            ushort version;
            var page = LoadJson(storage.Pages, key, writeBatch.Value, out version);
            if (page == null)
                return -1;

            buffer = page.Value<byte[]>("data"); // TODO split page table?
            return buffer.Length;
        }

        public FileHeader ReadFile(string filename)
        {
            var fileNameHash = HashKey(filename);
            var key = CreateKey(filename, fileNameHash);

            ushort version;
            var file = LoadJson(storage.Files, key, writeBatch.Value, out version);
            if (file == null)
                return null;

            return new FileHeader
                   {
                       Name = file.Value<string>("name"),
                       TotalSize = file.Value<long>("total_size"),
                       UploadedSize = file.Value<long>("uploaded_size"),
                       Metadata = RetrieveMetadata(file)
                   };
        }

        public FileAndPages GetFile(string filename, int start, int pagesToLoad)
        {
            var fileNameHash = HashKey(filename);
            var key = CreateKey(filename, fileNameHash);

            ushort version;
            var file = LoadJson(storage.Files, key, writeBatch.Value, out version);
            if (file == null)
                throw new FileNotFoundException("Could not find file: " + filename);

            var fileInformation = new FileAndPages
                                  {
                                      TotalSize = file.Value<long>("total_size"),
                                      UploadedSize = file.Value<long>("uploaded_size"),
                                      Metadata = RetrieveMetadata(file),
                                      Name = filename,
                                      Start = start
                                  };

            if (pagesToLoad > 0)
            {
                var usageByFileNameAndPosition = storage.Usage.GetIndex(Tables.Usage.Indices.ByFileNameAndPosition);
                var fileNameAndPositionKey = CreateKey(filename, fileNameHash, start);

                using (var iterator = usageByFileNameAndPosition.MultiRead(Snapshot, fileNameAndPositionKey))
                {
                    if (iterator.Seek(Slice.BeforeAllKeys))
                    {
                        do
                        {
                            var id = iterator.CurrentKey.ToString();
                            var pageInformation = LoadJson(storage.Usage, id, writeBatch.Value, version: out version);
                            
                            fileInformation.Pages.Add(new PageInformation
                                                      {
                                                          Id = pageInformation.Value<int>("page_id"),
                                                          Size = pageInformation.Value<int>("page_size")
                                                      });
                        }
                        while (iterator.MoveNext() && fileInformation.Pages.Count < pagesToLoad);
                    }
                }
            }

            return fileInformation;
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

        private static string ToQueryString(NameValueCollection metadata)
        {
            var sb = new StringBuilder();
            foreach (var key in metadata.AllKeys)
            {
                var values = metadata.GetValues(key);
                if (values == null)
                    continue;

                foreach (var value in values)
                {
                    sb.Append(key)
                      .Append("=")
                      .Append(Uri.EscapeDataString(value))
                      .Append("&");
                }
            }

            if (sb.Length > 0)
                sb.Length = sb.Length - 1;

            return sb.ToString();
        }

        private static NameValueCollection RetrieveMetadata(RavenJObject file)
        {
            var metadataAsString = file.Value<string>("metadata");
            var metadata = HttpUtility.ParseQueryString(metadataAsString);
            metadata["ETag"] = "\"" + Guid.Parse(file.Value<string>("Etag"))  + "\"";

            return metadata;
        }

    }
}