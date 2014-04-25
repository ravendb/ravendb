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
using Raven.Database.Server.RavenFS.Storage.Exceptions;
using Raven.Database.Server.RavenFS.Storage.Voron.Impl;
using Raven.Database.Server.RavenFS.Synchronization.Rdc;
using Raven.Database.Server.RavenFS.Util;
using Raven.Json.Linq;

using Voron;
using Voron.Impl;
using Raven.Client.RavenFS;
using System.Diagnostics;

namespace Raven.Database.Server.RavenFS.Storage.Voron
{
    public class StorageActionsAccessor : StorageActionsBase, IStorageActionsAccessor
    {
        private readonly TableStorage storage;

        private readonly Reference<WriteBatch> writeBatch;

        public StorageActionsAccessor(TableStorage storage, Reference<WriteBatch> writeBatch, SnapshotReader snapshot, IdGenerator generator, IBufferPool bufferPool)
            : base(snapshot, generator, bufferPool)
        {
            this.storage = storage;
            this.writeBatch = writeBatch;
        }

        public void Dispose()
        {
            // nothing to do here
        }

        public void Commit()
        {
            // nothing to do here
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
            var pageData = storage.Pages.GetIndex(Tables.Pages.Indices.Data);

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

                storage.Pages.Add(writeBatch.Value, id, page, version);

                return page.Value<int>("id");
            }

            var newId = IdGenerator.GetNextIdForTable(storage.Pages);
            var newKey = CreateKey(newId);

            var newPage = new RavenJObject
                   {
                       {"id", newId},
                       {"page_strong_hash", hashKey.Strong},
                       {"page_weak_hash", hashKey.Weak},
                       {"usage_count", 0}
                   };

            storage.Pages.Add(writeBatch.Value, newKey, newPage, 0);
            pageData.Add(writeBatch.Value, newKey, buffer, 0);
            pageByKey.Add(writeBatch.Value, key, newKey);

            return newId;
        }

        public void PutFile(string filename, long? totalSize, RavenJObject metadata, bool tombstone = false)
        {
            var filesByEtag = storage.Files.GetIndex(Tables.Files.Indices.ByEtag);

            var key = CreateKey(filename);

            if (!metadata.ContainsKey("ETag"))
                throw new InvalidOperationException(string.Format("Metadata of file {0} does not contain 'ETag' key", filename));

            ushort version;
            var existingFile = LoadJson(storage.Files, key, writeBatch.Value, out version);

            var innerMetadata = new RavenJObject(metadata);
            var etag = innerMetadata.Value<Guid>("ETag");
            innerMetadata.Remove("ETag");

            var file = new RavenJObject
                       {
                           { "name", filename }, 
                           { "total_size", totalSize }, 
                           { "uploaded_size", 0 }, 
                           { "etag", new RavenJValue(etag) }, 
                           { "metadata", innerMetadata }
                       };

            storage.Files.Add(writeBatch.Value, key, file, version);

            if (existingFile != null)
            {
                filesByEtag.Delete(writeBatch.Value, CreateKey(existingFile.Value<Guid>("etag")));
            }

            filesByEtag.Add(writeBatch.Value, CreateKey(etag), key);

            if (tombstone)
                return;

            var fileCount = storage.Files.GetIndex(Tables.Files.Indices.Count);
            fileCount.Add(writeBatch.Value, key, key);
        }

        public void AssociatePage(string filename, int pageId, int pagePositionInFile, int pageSize)
        {
            var usageByFileName = storage.Usage.GetIndex(Tables.Usage.Indices.ByFileName);
            var usageByFileNameAndPosition = storage.Usage.GetIndex(Tables.Usage.Indices.ByFileNameAndPosition);

            var key = CreateKey(filename);
            ushort version;

            var file = LoadFileByKey(key, out version);
            var totalSize = file.Value<long?>("total_size");
            var uploadedSize = file.Value<int>("uploaded_size");

            if (totalSize != null && totalSize >= 0 && uploadedSize + pageSize > totalSize)
                throw new InvalidDataException("Try to upload more data than the file was allocated for (" + totalSize +
                                               ") and new size would be: " + (uploadedSize + pageSize));

            file["uploaded_size"] = uploadedSize + pageSize;

            // using chunked encoding, we don't know what the size is
            // we use negative values here for keeping track of the unknown size
            if (totalSize == null || totalSize < 0)
            {
                var actualSize = totalSize ?? 0;
                file["total_size"] = actualSize - pageSize;
            }

            storage.Files.Add(writeBatch.Value, key, file, version);

            var id = IdGenerator.GetNextIdForTable(storage.Usage);
            var usageKey = CreateKey(id);

            var usage = new RavenJObject
                        {
                            { "id", id },
                            { "name", filename }, 
                            { "file_pos", pagePositionInFile }, 
                            { "page_id", pageId }, 
                            { "page_size", pageSize }
                        };

            storage.Usage.Add(writeBatch.Value, usageKey, usage, 0);
            usageByFileName.MultiAdd(writeBatch.Value, CreateKey(filename), usageKey);
            usageByFileNameAndPosition.Add(writeBatch.Value, CreateKey(filename, pagePositionInFile), usageKey, 0);
        }

        public int ReadPage(int pageId, byte[] buffer)
        {
            var key = CreateKey(pageId);
            var pageData = storage.Pages.GetIndex(Tables.Pages.Indices.Data);

            var result = pageData.Read(Snapshot, key, writeBatch.Value);
            if (result == null)
                return -1;

            result.Reader.Read(buffer, 0, buffer.Length);
            return result.Reader.Length;
        }

        public FileHeader ReadFile(string filename)
        {
            var key = CreateKey(filename);

            ushort version;
            var file = LoadJson(storage.Files, key, writeBatch.Value, out version);
            if (file == null)
                return null;

            return ConvertToFile(file);
        }

        public FileAndPages GetFile(string filename, int start, int pagesToLoad)
        {
            var key = CreateKey(filename);

            ushort version;
            var file = LoadJson(storage.Files, key, writeBatch.Value, out version);
            if (file == null)
                throw new FileNotFoundException("Could not find file: " + filename);

            var f = ConvertToFile(file);
            var fileInformation = new FileAndPages
                                  {
                                      TotalSize = f.TotalSize,
                                      Name = f.Name,
                                      Metadata = f.Metadata,
                                      UploadedSize = f.UploadedSize,
                                      Start = start
                                  };

            if (pagesToLoad > 0)
            {
                var usageByFileNameAndPosition = storage.Usage.GetIndex(Tables.Usage.Indices.ByFileNameAndPosition);

                using (var iterator = usageByFileNameAndPosition.Iterate(Snapshot, writeBatch.Value))
                {
                    if (iterator.Seek(CreateKey(filename, start)))
                    {
                        do
                        {
                            var id = iterator.CreateReaderForCurrent().ToStringValue();
                            var usage = LoadJson(storage.Usage, id, writeBatch.Value, out version);

                            var name = usage.Value<string>("name");
                            if (name.Equals(filename, StringComparison.InvariantCultureIgnoreCase) == false)
                                break;

                            fileInformation.Pages.Add(new PageInformation
                                                      {
                                                          Id = usage.Value<int>("page_id"),
                                                          Size = usage.Value<int>("page_size")
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
            using (var iterator = storage.Files.Iterate(Snapshot, writeBatch.Value))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
                    yield break;

                var count = 0;

                do
                {
                    ushort version;

                    var id = iterator.CurrentKey.ToString();
                    var file = LoadFileByKey(id, out version);
                    yield return ConvertToFile(file);

                    count++;
                }
                while (iterator.MoveNext() && count < size);
            }
        }

        public IEnumerable<FileHeader> GetFilesAfter(Guid etag, int take)
        {
            var key = CreateKey(etag);

            var filesByEtag = storage.Files.GetIndex(Tables.Files.Indices.ByEtag);
            using (var iterator = filesByEtag.Iterate(Snapshot, writeBatch.Value))
            {
                if (!iterator.Seek(key))
                    yield break;

                var count = 0;

                do
                {
                    var iteratorKey = iterator.CurrentKey.ToString();
                    if (iteratorKey.Equals(key))
                        continue;

                    ushort version;
                    var id = iterator.CreateReaderForCurrent().ToStringValue();
                    var file = LoadFileByKey(id, out version);
                    yield return ConvertToFile(file);

                    count++;
                }
                while (iterator.MoveNext() && count < take);
            }
        }

        public void Delete(string filename)
        {
            DeleteUsage(filename);
            DeleteFile(filename);
        }

        public void UpdateFileMetadata(string filename, RavenJObject metadata)
        {           
            var key = CreateKey(filename);

            ushort version;
            var file = LoadJson(storage.Files, key, writeBatch.Value, out version);
            if (file == null)
                throw new FileNotFoundException(filename);

            if (!metadata.ContainsKey("ETag"))
                throw new InvalidOperationException(string.Format("Metadata of file {0} does not contain 'ETag' key", filename));

            var innerMetadata = new RavenJObject(metadata);
            var etag = innerMetadata.Value<Guid>("ETag");
            innerMetadata.Remove("ETag");

            var existingMetadata = (RavenJObject) file["metadata"];
            if (existingMetadata.ContainsKey("Content-MD5"))
                innerMetadata["Content-MD5"] = existingMetadata["Content-MD5"];

            var oldEtag = file.Value<Guid>("etag");

            file["etag"] = new RavenJValue(etag);
            file["metadata"] = innerMetadata;

            storage.Files.Add(writeBatch.Value, key, file, version);

            var filesByEtag = storage.Files.GetIndex(Tables.Files.Indices.ByEtag);

            filesByEtag.Delete(writeBatch.Value, CreateKey(oldEtag));
            filesByEtag.Add(writeBatch.Value, CreateKey(etag), key);
        }

        public void CompleteFileUpload(string filename)
        {
            var key = CreateKey(filename);

            ushort version;
            var file = LoadJson(storage.Files, key, writeBatch.Value, out version);
            if (file == null)
                throw new FileNotFoundException(filename);

            var totalSize = file.Value<long?>("total_size") ?? 0;
            file["total_size"] = Math.Abs(totalSize);

            storage.Files.Add(writeBatch.Value, key, file, version);
        }

        public int GetFileCount()
        {
            var fileCount = storage.Files.GetIndex(Tables.Files.Indices.Count);
            return Convert.ToInt32(storage.GetEntriesCount(fileCount));
        }

		public void DecrementFileCount(string nameOfFileThatShouldNotBeCounted)
        {
			var fileCount = storage.Files.GetIndex(Tables.Files.Indices.Count);

			fileCount.Delete(writeBatch.Value, CreateKey(nameOfFileThatShouldNotBeCounted));
        }

        public void RenameFile(string filename, string rename, bool commitPeriodically = false)
        {
            ushort version;
            ushort? renameVersion;
            var renameKey = CreateKey(rename);

            if (storage.Files.Contains(Snapshot, renameKey, writeBatch.Value, out renameVersion))
                throw new FileExistsException(string.Format("Cannot rename '{0}' to '{1}'. Rename '{1}' exists.", filename, rename));

            var key = CreateKey(filename);
            var file = LoadJson(storage.Files, key, writeBatch.Value, out version);
            if (file == null)
                throw new FileNotFoundException("Could not find file: " + filename);

            RenameUsage(filename, rename, commitPeriodically);
            DeleteFile(filename);

            file["name"] = rename;
            storage.Files.Add(writeBatch.Value, renameKey, file, renameVersion ?? 0);

			var fileCount = storage.Files.GetIndex(Tables.Files.Indices.Count);
			fileCount.Add(writeBatch.Value, renameKey, renameKey);

			var filesByEtag = storage.Files.GetIndex(Tables.Files.Indices.ByEtag);

			filesByEtag.Add(writeBatch.Value, CreateKey(file.Value<Guid>("etag")), renameKey);
        }

        private void RenameUsage(string fileName, string rename, bool commitPeriodically)
        {
            var oldKey = CreateKey(fileName);
            var newKey = CreateKey(rename);

            var usageByFileName = storage.Usage.GetIndex(Tables.Usage.Indices.ByFileName);
            var usageByFileNameAndPosition = storage.Usage.GetIndex(Tables.Usage.Indices.ByFileNameAndPosition);

            using (var iterator = usageByFileName.MultiRead(Snapshot, oldKey))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys))
                    return;

                var count = 0;

                do
                {
                    var usageId = iterator.CurrentKey.ToString();
                    ushort version;
                    var usage = LoadJson(storage.Usage, usageId, writeBatch.Value, out version);

                    usage["name"] = rename;
                    var position = usage.Value<int>("file_pos");

                    storage.Usage.Add(writeBatch.Value, usageId, usage, version);

                    usageByFileName.MultiDelete(writeBatch.Value, oldKey, usageId);
                    usageByFileNameAndPosition.Delete(writeBatch.Value, CreateKey(fileName, position));

                    usageByFileName.MultiAdd(writeBatch.Value, newKey, usageId);
                    usageByFileNameAndPosition.Add(writeBatch.Value, CreateKey(rename, position), usageId);

                    if (commitPeriodically && count++ > 1000)
                    {
                        PulseTransaction();
                        count = 0;
                    }
                }
                while (iterator.MoveNext());
            }
        }

        public RavenJObject GetConfig(string name)
        {
            var key = CreateKey(name);
            ushort version;
            var config = LoadJson(storage.Config, key, writeBatch.Value, out version);
            if (config == null)
                throw new FileNotFoundException("Could not find config: " + name);

            var metadata = config.Value<RavenJObject>("metadata");
            return metadata;
        }

        public void SetConfig(string name, RavenJObject metadata)
        {            
            var key = CreateKey(name);
            ushort version;
            var config = LoadJson(storage.Config, key, writeBatch.Value, out version) ?? new RavenJObject();

            config["metadata"] = metadata;
            config["name"] = name;

            storage.Config.Add(writeBatch.Value, key, config, version);
        }

        public void DeleteConfig(string name)
        {
            var key = CreateKey(name);

            storage.Config.Delete(writeBatch.Value, key);
        }

        public IEnumerable<SignatureLevels> GetSignatures(string name)
        {
            var key = CreateKey(name);

            var signaturesByName = storage.Signatures.GetIndex(Tables.Signatures.Indices.ByName);

            using (var iterator = signaturesByName.MultiRead(Snapshot, key))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys))
                    yield break;

                do
                {
                    var id = iterator.CurrentKey.ToString();
                    var signature = LoadSignatureByKey(id);
                    yield return ConvertToSignature(signature);
                }
                while (iterator.MoveNext());
            }
        }

        public void ClearSignatures(string name)
        {
            var key = CreateKey(name);
            var signaturesByName = storage.Signatures.GetIndex(Tables.Signatures.Indices.ByName);

            using (var iterator = signaturesByName.MultiRead(Snapshot, key))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys))
                    return;

                do
                {
                    var id = iterator.CurrentKey.ToString();
                    RemoveSignature(id, name);
                }
                while (iterator.MoveNext());
            }
        }

        public long GetSignatureSize(int id, int level)
        {
            var key = CreateKey(id);
            var signatureData = storage.Signatures.GetIndex(Tables.Signatures.Indices.Data);

            var result = signatureData.Read(Snapshot, key, writeBatch.Value);
            if (result == null)
                throw new InvalidOperationException("Could not find signature with id " + id + " and level " + level);

            return result.Reader.Length;
        }

        public void GetSignatureStream(int id, int level, Action<Stream> action)
        {
            var key = CreateKey(id);
            var signatureData = storage.Signatures.GetIndex(Tables.Signatures.Indices.Data);

            var result = signatureData.Read(Snapshot, key, writeBatch.Value);
            if (result == null)
                throw new InvalidOperationException("Could not find signature with id " + id + " and level " + level);

            using (var stream = result.Reader.AsStream())
            {
                action(stream);
            }
        }

        public void AddSignature(string name, int level, Action<Stream> action)
        {
            var signatureData = storage.Signatures.GetIndex(Tables.Signatures.Indices.Data);
            var signaturesByName = storage.Signatures.GetIndex(Tables.Signatures.Indices.ByName);

            var id = IdGenerator.GetNextIdForTable(storage.Signatures);
            var key = CreateKey(id);

            var signature = new RavenJObject
                            {
                                { "id", id }, 
                                { "name", name }, 
                                { "level", level }, 
                                { "created_at", DateTime.UtcNow }
                            };

            var stream = CreateStream();

            action(stream);
            stream.Position = 0;

            storage.Signatures.Add(writeBatch.Value, key, signature, 0);
            signatureData.Add(writeBatch.Value, key, stream, 0);
            signaturesByName.MultiAdd(writeBatch.Value, CreateKey(name), key);
        }

        public IEnumerable<string> GetConfigNames(int start, int pageSize)
        {
            using (var iterator = storage.Config.Iterate(Snapshot, writeBatch.Value))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
                    yield break;

                var count = 0;

                do
                {
                    var config = iterator
                        .CreateReaderForCurrent()
                        .AsStream()
                        .ToJObject();

                    yield return config.Value<string>("name");
                    count++;
                }
                while (iterator.MoveNext() && count < pageSize);
            }
        }

        public bool ConfigExists(string name)
        {
            var key = CreateKey(name);

            return storage.Config.Contains(Snapshot, key, writeBatch.Value);
        }

        public IList<RavenJObject> GetConfigsStartWithPrefix(string prefix, int start, int take)
        {
            var key = CreateKey(prefix);
            var result = new List<RavenJObject>();

            using (var iterator = storage.Config.Iterate(Snapshot, writeBatch.Value))
            {
                if (!iterator.Seek(key) || !iterator.Skip(start))
                    return result;

                var count = 0;

                do
                {
                    var config = iterator.CreateReaderForCurrent()
                                         .AsStream()
                                         .ToJObject();

                    var metadata = config.Value<RavenJObject>("metadata");
                    var name = config.Value<string>("name");
                    if (name == null || name.StartsWith(key, StringComparison.InvariantCultureIgnoreCase) == false)
                        break;

                    result.Add(metadata);

                    count++;
                } while (iterator.MoveNext() && count < take);
            }

            return result;
        }

        public IList<string> GetConfigNamesStartingWithPrefix(string prefix, int start, int take, out int total)
        {
            total = 0;
            var results = new List<string>();

            var key = CreateKey(prefix);

            using (var iterator = storage.Config.Iterate(Snapshot, writeBatch.Value))
            {
                if (!iterator.Seek(key))
                    return results;

                var skippedCount = 0;
                for (var i = 0; i < start; i++)
                {
                    if (iterator.MoveNext() == false)
                    {
                        total = skippedCount;
                        return results;
                    }

                    skippedCount++;
                }

                var count = 0;

                do
                {
                    if (count < take)
                    {
                        var config = iterator
                            .CreateReaderForCurrent()
                            .AsStream()
                            .ToJObject();

                        results.Add(config.Value<string>("name"));
                    }

                    count++;
                } while (iterator.MoveNext());

                total = skippedCount + count;
            }

            return results;
        }

        private void RemoveSignature(string id, string name)
        {
            var signatureData = storage.Signatures.GetIndex(Tables.Signatures.Indices.Data);
            var signaturesByName = storage.Signatures.GetIndex(Tables.Signatures.Indices.ByName);

            signaturesByName.MultiDelete(writeBatch.Value, CreateKey(name), id);
            signatureData.Delete(writeBatch.Value, id);
            storage.Signatures.Delete(writeBatch.Value, id);
        }

        private void DeleteFile(string fileName)
        {
            var key = CreateKey(fileName);

            var fileCount = storage.Files.GetIndex(Tables.Files.Indices.Count);
            var filesByEtag = storage.Files.GetIndex(Tables.Files.Indices.ByEtag);

            ushort version;
            var file = LoadJson(storage.Files, key, writeBatch.Value, out version);

            if (file == null)
                return;

            var etag = file.Value<Guid>("etag");

            storage.Files.Delete(writeBatch.Value, key, version);

            fileCount.Delete(writeBatch.Value, key);
            filesByEtag.Delete(writeBatch.Value, CreateKey(etag));
        }

        private void DeleteUsage(string fileName)
        {
            var key = CreateKey(fileName);

            var usageByFileName = storage.Usage.GetIndex(Tables.Usage.Indices.ByFileName);
            var usageByFileNameAndPosition = storage.Usage.GetIndex(Tables.Usage.Indices.ByFileNameAndPosition);

            using (var iterator = usageByFileName.MultiRead(Snapshot, key))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys))
                    return;

                var count = 0;

                do
                {
                    var id = iterator.CurrentKey.ToString();
                    ushort version;
                    var usage = LoadJson(storage.Usage, id, writeBatch.Value, out version);
                    var pageId = usage.Value<int>("page_id");
                    var position = usage.Value<int>("file_pos");

                    DeletePage(pageId);

                    storage.Usage.Delete(writeBatch.Value, id);
                    usageByFileName.MultiDelete(writeBatch.Value, key, id);
                    usageByFileNameAndPosition.Delete(writeBatch.Value, CreateKey(fileName, position));

                    if (count++ <= 1000)
                    {
                        continue;
                    }

                    PulseTransaction();
                    count = 0;
                }
                while (iterator.MoveNext());
            }
        }

        private void DeletePage(int pageId)
        {
            var key = CreateKey(pageId);

            ushort version;
            var page = LoadJson(storage.Pages, key, writeBatch.Value, out version);
            var usageCount = page.Value<int>("usage_count");
            if (usageCount <= 1)
            {
                var pageData = storage.Pages.GetIndex(Tables.Pages.Indices.Data);
                var pagesByKey = storage.Pages.GetIndex(Tables.Pages.Indices.ByKey);

                var strongHash = page.Value<byte[]>("page_strong_hash");
                var weakHash = page.Value<int>("page_weak_hash");

                var hashKey = new HashKey
                              {
                                  Strong = strongHash,
                                  Weak = weakHash
                              };

                storage.Pages.Delete(writeBatch.Value, key, version);
                pageData.Delete(writeBatch.Value, key);
                pagesByKey.Delete(writeBatch.Value, ConvertToKey(hashKey));
            }
            else
            {
                page["usage_count"] = usageCount - 1;
                storage.Pages.Add(writeBatch.Value, key, page, version);
            }
        }


        private RavenJObject LoadFileByKey(string key, out ushort version)
        {
            var file = LoadJson(storage.Files, key, writeBatch.Value, out version);

            if (file == null)
                throw new FileNotFoundException("Could not find file: " + key);

            return file;
        }
        private RavenJObject LoadSignatureByKey(string key)
        {
            ushort version;
            var signature = LoadJson(storage.Signatures, key, writeBatch.Value, out version);

            if (signature == null)
                throw new FileNotFoundException("Could not find signature: " + key);

            return signature;
        }

        private static SignatureLevels ConvertToSignature(RavenJObject signature)
        {
            return new SignatureLevels
            {
                CreatedAt = signature.Value<DateTime>("created_at"),
                Id = signature.Value<int>("id"),
                Level = signature.Value<int>("level")
            };
        }

        private static FileHeader ConvertToFile(RavenJObject file)
        {
            var metadata = (RavenJObject)file["metadata"];
            metadata["ETag"] = file["etag"];

            return new FileHeader
                   {
                       Name = file.Value<string>("name"),
                       TotalSize = file.Value<long?>("total_size"),
                       UploadedSize = file.Value<long>("uploaded_size"),
                       Metadata = metadata,
                   };
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

    }
}