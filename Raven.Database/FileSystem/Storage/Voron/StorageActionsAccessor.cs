// -----------------------------------------------------------------------
//  <copyright file="StorageActionsAccessor.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.MEF;
using Raven.Abstractions.Util.Streams;
using Raven.Database.FileSystem.Infrastructure;
using Raven.Database.FileSystem.Plugins;
using Raven.Database.FileSystem.Storage.Exceptions;
using Raven.Database.FileSystem.Storage.Voron.Impl;
using Raven.Database.FileSystem.Synchronization.Rdc;
using Raven.Database.FileSystem.Util;
using Raven.Database.Storage.Voron;
using Raven.Json.Linq;

using Voron;
using Voron.Impl;
using RavenConstants = Raven.Abstractions.Data.Constants;

using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;

namespace Raven.Database.FileSystem.Storage.Voron
{
    internal class StorageActionsAccessor : StorageActionsBase, IStorageActionsAccessor
    {
        private readonly TableStorage storage;

        private readonly Reference<WriteBatch> writeBatch;
        private readonly UuidGenerator uuidGenerator;
        private readonly OrderedPartCollection<AbstractFileCodec> fileCodecs;

        public StorageActionsAccessor(TableStorage storage, Reference<WriteBatch> writeBatch, Reference<SnapshotReader> snapshot, IdGenerator generator, IBufferPool bufferPool, UuidGenerator uuidGenerator, OrderedPartCollection<AbstractFileCodec> fileCodecs)
            : base(snapshot, generator, bufferPool)
        {
            this.storage = storage;
            this.writeBatch = writeBatch;
            this.uuidGenerator = uuidGenerator;
            this.fileCodecs = fileCodecs;
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
            var key = (Slice)ConvertToKey(hashKey);

            var pageByKey = storage.Pages.GetIndex(Tables.Pages.Indices.ByKey);
            var pageData = storage.Pages.GetIndex(Tables.Pages.Indices.Data);

            var result = pageByKey.Read(Snapshot, key, writeBatch.Value);
            if (result != null)
            {
                var id = (Slice)result.Reader.ToStringValue();

                ushort version;
                var page = LoadJson(storage.Pages, id, writeBatch.Value, out version);
                if (page == null)
                    throw new InvalidOperationException(string.Format("Could not find page '{0}'. Probably data is corrupted.", id));

                var usageCount = page.Value<int>("usage_count");
                page["usage_count"] = usageCount + 1;

                storage.Pages.Add(writeBatch.Value, id, page, version);

                return page.Value<int>("id");
            }

            var newPageId = IdGenerator.GetNextIdForTable(storage.Pages);
            var newPageKeyString = CreateKey(newPageId);
            var newPageKey = (Slice)newPageKeyString;

            var newPage = new RavenJObject
                   {
                       {"id", newPageId},
                       {"page_strong_hash", hashKey.Strong},
                       {"page_weak_hash", hashKey.Weak},
                       {"usage_count", 1}
                   };

            storage.Pages.Add(writeBatch.Value, newPageKey, newPage, 0);

            var dataStream = CreateStream();

            using (var finalDataStream = fileCodecs.Aggregate((Stream)new UndisposableStream(dataStream),
                (current, codec) => codec.EncodePage(current)))
            {
                finalDataStream.Write(buffer, 0, size);
                finalDataStream.Flush();
            }

            dataStream.Position = 0;

            pageData.Add(writeBatch.Value, newPageKey, dataStream, 0);

            pageByKey.Add(writeBatch.Value, key, newPageKeyString);

            return newPageId;
        }

        public FileUpdateResult PutFile(string filename, long? totalSize, RavenJObject metadata, bool tombstone = false)
        {
            var filesByEtag = storage.Files.GetIndex(Tables.Files.Indices.ByEtag);

            var keyString = CreateKey(filename);
            var keySlice = (Slice) keyString;

            ushort version;
            var existingFile = LoadJson(storage.Files, keySlice, writeBatch.Value, out version);

            var newEtag = uuidGenerator.CreateSequentialUuid();

            metadata.Remove(RavenConstants.MetadataEtagField);

            var file = new RavenJObject
                       {
                           { "name", filename }, 
                           { "total_size", totalSize }, 
                           { "uploaded_size", 0 }, 
                           { "etag", newEtag.ToByteArray() }, 
                           { "metadata", metadata }
                       };

            storage.Files.Add(writeBatch.Value, keySlice, file, version);

            if (existingFile != null)
            {
                filesByEtag.Delete(writeBatch.Value, CreateKey(Etag.Parse(existingFile.Value<byte[]>("etag"))));
            }

            filesByEtag.Add(writeBatch.Value, (Slice)CreateKey(newEtag), keyString);

            if (tombstone == false)
            {
                var fileCount = storage.Files.GetIndex(Tables.Files.Indices.Count);
                fileCount.Add(writeBatch.Value, keySlice, keyString);
            }

            return new FileUpdateResult()
            {
                Etag = newEtag
            };
        }

        public void AssociatePage(string filename, int pageId, int pagePositionInFile, int pageSize)
        {
            var usageByFileName = storage.Usage.GetIndex(Tables.Usage.Indices.ByFileName);
            var usageByFileNameAndPosition = storage.Usage.GetIndex(Tables.Usage.Indices.ByFileNameAndPosition);

            var key = CreateKey(filename);
            var keySlice = (Slice)key;
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

            storage.Files.Add(writeBatch.Value, keySlice, file, version);

            var id = IdGenerator.GetNextIdForTable(storage.Usage);
            var usageKey = CreateKey(id);
            var usageKeySlice = (Slice)usageKey;

            var usage = new RavenJObject
                        {
                            { "id", id },
                            { "name", filename }, 
                            { "file_pos", pagePositionInFile }, 
                            { "page_id", pageId }, 
                            { "page_size", pageSize }
                        };

            storage.Usage.Add(writeBatch.Value, usageKeySlice, usage, 0);
            usageByFileName.MultiAdd(writeBatch.Value, (Slice)CreateKey(filename), usageKeySlice);
            usageByFileNameAndPosition.Add(writeBatch.Value, (Slice)CreateKey(filename, pagePositionInFile), usageKey, 0);
        }

        public int ReadPage(int pageId, byte[] buffer)
        {
            var key = (Slice)CreateKey(pageId);
            var pageData = storage.Pages.GetIndex(Tables.Pages.Indices.Data);

            var result = pageData.Read(Snapshot, key, writeBatch.Value);
            if (result == null)
                return -1;

            using (var stream = result.Reader.AsStream())
            {
                using (var decodedStream = fileCodecs.Aggregate(stream, (current, codec) => codec.DecodePage(current)))
                {
                    return decodedStream.Read(buffer, 0, buffer.Length);
                }
            }
        }

        public FileHeader ReadFile(string filename)
        {
            var key = (Slice)CreateKey(filename);

            ushort version;
            var file = LoadJson(storage.Files, key, writeBatch.Value, out version);
            if (file == null)
                return null;

            return ConvertToFile(file);
        }

        public FileAndPagesInformation GetFile(string filename, int start, int pagesToLoad)
        {
            var key = (Slice)CreateKey(filename);

            ushort version;
            var file = LoadJson(storage.Files, key, writeBatch.Value, out version);
            if (file == null)
                throw new FileNotFoundException("Could not find file: " + filename);

            var f = ConvertToFile(file);
            var fileInformation = new FileAndPagesInformation
                                  {
                                      TotalSize = f.TotalSize,
                                      Name = f.FullPath,
                                      Metadata = f.Metadata,
                                      UploadedSize = f.UploadedSize,
                                      Start = start
                                  };

            if (pagesToLoad > 0)
            {
                var usageByFileNameAndPosition = storage.Usage.GetIndex(Tables.Usage.Indices.ByFileNameAndPosition);

                using (var iterator = usageByFileNameAndPosition.Iterate(Snapshot, writeBatch.Value))
                {
                    if (iterator.Seek((Slice)CreateKey(filename, start)))
                    {
                        do
                        {
                            var id = (Slice)iterator.CreateReaderForCurrent().ToStringValue();
                            var usage = LoadJson(storage.Usage, id, writeBatch.Value, out version);

                            var name = usage.Value<string>("name");
                            if (name.Equals(filename, StringComparison.InvariantCultureIgnoreCase) == false)
                                break;

                            fileInformation.Pages.Add(new PageInformation
                                                      {
                                                          Id = usage.Value<int>("page_id"),
                                                          Size = usage.Value<int>("page_size"),
                                                          PositionInFile = usage.Value<int>("file_pos")
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

        public IEnumerable<FileHeader> GetFilesAfter(Etag etag, int take)
        {
            var key = CreateKey(etag);

            var filesByEtag = storage.Files.GetIndex(Tables.Files.Indices.ByEtag);
            using (var iterator = filesByEtag.Iterate(Snapshot, writeBatch.Value))
            {
                if (!iterator.Seek((Slice)key))
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

        public IEnumerable<FileHeader> GetFilesStartingWith(string namePrefix, int start, int take)
        {
            if (string.IsNullOrEmpty(namePrefix))
                throw new ArgumentNullException("namePrefix");
            if (start < 0)
                throw new ArgumentException("must have zero or positive value", "start");
            if (take < 0)
                throw new ArgumentException("must have zero or positive value", "take");

            if (take == 0)
                yield break;

            using (var iterator = storage.Files.Iterate(Snapshot, writeBatch.Value))
            {
                iterator.RequiredPrefix = (Slice)namePrefix.ToLowerInvariant();
                if (iterator.Seek(iterator.RequiredPrefix) == false || iterator.Skip(start) == false)
                    yield break;

                var fetchedCount = 0;
                do
                {
                    var key = iterator.CurrentKey.ToString();
                    
                    ushort version;
                    var file = LoadFileByKey(key, out version);
                    
                    fetchedCount++;

                    yield return ConvertToFile(file);
                } while (iterator.MoveNext() && fetchedCount < take);
            }
        }

        public void Delete(string filename)
        {
            DeleteUsage(filename);
            DeleteFile(filename);
        }

        public FileUpdateResult UpdateFileMetadata(string filename, RavenJObject metadata, Etag etag)
        {
            var key = CreateKey(filename);
            var keySlice = (Slice)key;

            ushort version;
            var file = LoadJson(storage.Files, keySlice, writeBatch.Value, out version);
            if (file == null)
                throw new FileNotFoundException(filename);

            var existingEtag = EnsureDocumentEtagMatch(filename, etag, file);

            var newEtag = uuidGenerator.CreateSequentialUuid();
            metadata.Remove(RavenConstants.MetadataEtagField);

            var existingMetadata = (RavenJObject) file["metadata"];

            if (!metadata.ContainsKey("Content-MD5") && existingMetadata.ContainsKey("Content-MD5"))
                metadata["Content-MD5"] = existingMetadata["Content-MD5"];
            if (!metadata.ContainsKey(RavenConstants.FileSystem.RavenFsSize) && existingMetadata.ContainsKey(RavenConstants.FileSystem.RavenFsSize))
                metadata[RavenConstants.FileSystem.RavenFsSize] = existingMetadata[RavenConstants.FileSystem.RavenFsSize];

            file["etag"] = newEtag.ToByteArray();
            file["metadata"] = metadata;

            storage.Files.Add(writeBatch.Value, keySlice, file, version);

            var filesByEtag = storage.Files.GetIndex(Tables.Files.Indices.ByEtag);

            filesByEtag.Delete(writeBatch.Value, CreateKey(existingEtag));
            filesByEtag.Add(writeBatch.Value, (Slice)CreateKey(newEtag), key);

            return new FileUpdateResult()
            {
                PrevEtag = existingEtag,
                Etag = newEtag
            };
        }

        public void CompleteFileUpload(string filename)
        {
            var key = (Slice)CreateKey(filename);

            ushort version;
            var file = LoadJson(storage.Files, key, writeBatch.Value, out version);
            if (file == null)
                throw new FileNotFoundException(filename);

            var totalSize = file.Value<long?>("total_size") ?? 0;
            var uploadedSize = file.Value<long?>("uploaded_size") ?? 0;

            if (uploadedSize < totalSize )
                file["total_size"] = Math.Abs(uploadedSize);
            else
                file["total_size"] = Math.Abs(totalSize);            

            storage.Files.Add(writeBatch.Value, key, file, version);
        }

        public Etag GetLastEtag()
        {
            var filesByEtag = storage.Files.GetIndex(Tables.Files.Indices.ByEtag);

            using (var it = filesByEtag.Iterate(Snapshot, writeBatch.Value))
            {
                if (it.Seek(Slice.AfterAllKeys) == false)
                    return Etag.Empty;

                Etag result = null;
                var maxKey = it.CurrentKey.ToString();
                if (!Etag.TryParse(maxKey, out result))
                    return Etag.Empty;

                return result;           
            }
        }

        public bool IsNested { get; set; }

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
            var renameKeySlice = (Slice)renameKey;

            if (storage.Files.Contains(Snapshot, renameKeySlice, writeBatch.Value, out renameVersion))
                throw new FileExistsException(string.Format("Cannot rename '{0}' to '{1}'. Rename '{1}' exists.", filename, rename));

            var key = (Slice)CreateKey(filename);
            var file = LoadJson(storage.Files, key, writeBatch.Value, out version);
            if (file == null)
                throw new FileNotFoundException("Could not find file: " + filename);

            RenameUsage(filename, rename, commitPeriodically);
            DeleteFile(filename);

            file["name"] = rename;
            storage.Files.Add(writeBatch.Value, renameKeySlice, file, renameVersion ?? 0);

            var fileCount = storage.Files.GetIndex(Tables.Files.Indices.Count);
            fileCount.Add(writeBatch.Value, renameKey, renameKey);

            var filesByEtag = storage.Files.GetIndex(Tables.Files.Indices.ByEtag);

            filesByEtag.Add(writeBatch.Value, (Slice)CreateKey(Etag.Parse(file.Value<byte[]>("etag"))), renameKey);
        }

        private void RenameUsage(string fileName, string rename, bool commitPeriodically)
        {
            var oldKey = (Slice)CreateKey(fileName);
            var newKey = (Slice)CreateKey(rename);

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
                    var usageIdSlice = (Slice)usageId;

                    ushort version;
                    var usage = LoadJson(storage.Usage, usageIdSlice, writeBatch.Value, out version);

                    usage["name"] = rename;
                    var position = usage.Value<int>("file_pos");

                    storage.Usage.Add(writeBatch.Value, usageIdSlice, usage, version);

                    usageByFileName.MultiDelete(writeBatch.Value, oldKey, usageIdSlice);
                    usageByFileNameAndPosition.Delete(writeBatch.Value, CreateKey(fileName, position));

                    usageByFileName.MultiAdd(writeBatch.Value, newKey, usageIdSlice);
                    usageByFileNameAndPosition.Add(writeBatch.Value, (Slice)CreateKey(rename, position), usageId);

                    if (commitPeriodically && count++ > 1000)
                    {
                        PulseTransaction();
                        count = 0;
                    }
                }
                while (iterator.MoveNext());
            }
        }

        public void CopyFile(string sourceFilename, string targetFilename, bool commitPeriodically = false)
        {
            ushort version;
            ushort? fileVersion;
            var targetKey = CreateKey(targetFilename);

            if (storage.Files.Contains(Snapshot, targetKey, writeBatch.Value, out fileVersion))
                throw new FileExistsException(string.Format("Cannot copy '{0}' to '{1}'. File '{1}' exists.", sourceFilename, targetFilename));

            var key = CreateKey(sourceFilename);
            var file = LoadJson(storage.Files, key, writeBatch.Value, out version);
            if (file == null)
                throw new FileNotFoundException("Could not find file: " + sourceFilename);

            var newEtag = uuidGenerator.CreateSequentialUuid();
            file["etag"] = newEtag.ToByteArray();
            file.Value<RavenJObject>("metadata").Remove(RavenConstants.MetadataEtagField);

            CopyUsage(sourceFilename, targetFilename, commitPeriodically);

            file["name"] = targetFilename;
            storage.Files.Add(writeBatch.Value, targetKey, file, fileVersion ?? 0);

            var fileCount = storage.Files.GetIndex(Tables.Files.Indices.Count);
            fileCount.Add(writeBatch.Value, targetKey, targetKey);

            var filesByEtag = storage.Files.GetIndex(Tables.Files.Indices.ByEtag);
            filesByEtag.Add(writeBatch.Value, CreateKey(Etag.Parse(file.Value<byte[]>("etag"))), targetKey);
        }

        private void CopyUsage(string sourceFilename, string targetFilename, bool commitPeriodically)
        {
            var oldKey = CreateKey(sourceFilename);
            var newKey = CreateKey(targetFilename);

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
                    var newId = IdGenerator.GetNextIdForTable(storage.Usage);
                    var position = usage.Value<int>("file_pos");

                    var newUsage = new RavenJObject
                        {
                            { "id", newId },
                            { "name", targetFilename }, 
                            { "file_pos", position }, 
                            { "page_id", usage.Value<int>("page_id") }, 
                            { "page_size", usage.Value<int>("page_size") }
                        };

                    var newUsageId = CreateKey(newId);

                    storage.Usage.Add(writeBatch.Value, newUsageId, newUsage);

                    usageByFileName.MultiAdd(writeBatch.Value, newKey, newUsageId);
                    usageByFileNameAndPosition.Add(writeBatch.Value, CreateKey(targetFilename, position), newUsageId);

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
            var key = (Slice)CreateKey(name);
            ushort version;
            var config = LoadJson(storage.Config, key, writeBatch.Value, out version);
            if (config == null)
                throw new FileNotFoundException("Could not find config: " + name);

            var metadata = config.Value<RavenJObject>("metadata");
            return metadata;
        }

        public void SetConfig(string name, RavenJObject metadata)
        {
            var key = (Slice)CreateKey(name);
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
            var key = (Slice)CreateKey(name);

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
            var key = (Slice)CreateKey(name);
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
            var key = (Slice)CreateKey(id);
            var signatureData = storage.Signatures.GetIndex(Tables.Signatures.Indices.Data);

            var result = signatureData.Read(Snapshot, key, writeBatch.Value);
            if (result == null)
                throw new InvalidOperationException("Could not find signature with id " + id + " and level " + level);

            return result.Reader.Length;
        }

        public void GetSignatureStream(int id, int level, Action<Stream> action)
        {
            var key = (Slice)CreateKey(id);
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
            var key = (Slice)CreateKey(id);

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
            signaturesByName.MultiAdd(writeBatch.Value, (Slice)CreateKey(name), key);
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
            var key = (Slice)CreateKey(name);

            return storage.Config.Contains(Snapshot, key, writeBatch.Value);
        }

        public IList<RavenJObject> GetConfigsStartWithPrefix(string prefix, int start, int take, out int total)
        {

            var results = new List<RavenJObject>();

            var totalRef = new Reference<int>();

            foreach (var config in GetConfigsWithPrefix(prefix, start, take, totalRef))
            {
                var metadata = config.Value<RavenJObject>("metadata");
                results.Add(metadata);;
            }

            total = totalRef.Value;

            return results;
        }

        public IList<string> GetConfigNamesStartingWithPrefix(string prefix, int start, int take, out int total)
        {
            var results = new List<string>();
            var totalRef = new Reference<int>();

            foreach (var config in GetConfigsWithPrefix(prefix, start, take, totalRef))
            {
                var configName = config.Value<string>("name");
                results.Add(configName);
            }

            total = totalRef.Value;

            return results;
        }

        private IEnumerable<RavenJObject> GetConfigsWithPrefix(string prefix, int start, int take, Reference<int> totalCount)
        {
            var key = (Slice) CreateKey(prefix);

            using (var iterator = storage.Config.Iterate(Snapshot, writeBatch.Value))
            {
                if (!iterator.Seek(key))
                    yield break;

                var skippedCount = 0;
                for (var i = 0; i < start; i++)
                {
                    skippedCount++;

                    if (iterator.MoveNext() == false || iterator.CurrentKey.StartsWith(key) == false)
                    {
                        totalCount.Value = skippedCount;
                        yield break;
                    }
                }

                var count = 0;

                do
                {
                    var config = iterator
                            .CreateReaderForCurrent()
                            .AsStream()
                            .ToJObject();

                    var configName = config.Value<string>("name");

                    if (configName.StartsWith(prefix) == false)
                        break;

                    if (count < take)
                    {
                        yield return config;
                    }

                    count++;
                } while (iterator.MoveNext());

                totalCount.Value = skippedCount + count;
            }
        }

        private void RemoveSignature(string id, string name)
        {
            var idSlice = (Slice)id;
            var nameSlice = (Slice)CreateKey(name);

            var signatureData = storage.Signatures.GetIndex(Tables.Signatures.Indices.Data);
            var signaturesByName = storage.Signatures.GetIndex(Tables.Signatures.Indices.ByName);

            signaturesByName.MultiDelete(writeBatch.Value, nameSlice, idSlice);
            signatureData.Delete(writeBatch.Value, idSlice);
            storage.Signatures.Delete(writeBatch.Value, idSlice);
        }

        private void DeleteFile(string fileName)
        {
            var key = (Slice) CreateKey(fileName);

            var fileCount = storage.Files.GetIndex(Tables.Files.Indices.Count);
            var filesByEtag = storage.Files.GetIndex(Tables.Files.Indices.ByEtag);

            ushort version;
            var file = LoadJson(storage.Files, key, writeBatch.Value, out version);

            if (file == null)
                return;

            var etag = Etag.Parse(file.Value<byte[]>("etag"));

            storage.Files.Delete(writeBatch.Value, key, version);

            fileCount.Delete(writeBatch.Value, key);
            filesByEtag.Delete(writeBatch.Value, CreateKey(etag));
        }

        private void DeleteUsage(string fileName)
        {
            var key = (Slice)CreateKey(fileName);

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
                    var idSlice = (Slice)id;

                    ushort version;
                    var usage = LoadJson(storage.Usage, idSlice, writeBatch.Value, out version);
                    var pageId = usage.Value<int>("page_id");
                    var position = usage.Value<int>("file_pos");

                    DeletePage(pageId);

                    storage.Usage.Delete(writeBatch.Value, id);
                    usageByFileName.MultiDelete(writeBatch.Value, key, idSlice);
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
            var key = (Slice)CreateKey(pageId);

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
            var file = LoadJson(storage.Files, (Slice)key, writeBatch.Value, out version);
            if (file == null)
                throw new FileNotFoundException("Could not find file: " + key);

            return file;
        }
        private RavenJObject LoadSignatureByKey(string key)
        {
            ushort version;
            var signature = LoadJson(storage.Signatures, (Slice)key, writeBatch.Value, out version);
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
            metadata[RavenConstants.MetadataEtagField] = Etag.Parse(file.Value<byte[]>("etag")).ToString();
            // To avoid useless handling of conversions for special headers we return the same type we stored.
            if (metadata.ContainsKey(RavenConstants.LastModified))
                metadata[RavenConstants.LastModified] = metadata.Value<DateTimeOffset>(RavenConstants.LastModified);            

            return new FileHeader (file.Value<string>("name"), metadata )
                   {
                       TotalSize = file.Value<long?>("total_size"),
                       UploadedSize = file.Value<long>("uploaded_size"),
                   };
        }

        private Etag EnsureDocumentEtagMatch(string key, Etag etag, RavenJObject file)
        {
            var existingEtag = Etag.Parse(file.Value<byte[]>("etag"));

            if (etag != null)
            {
                if (existingEtag != etag)
                {
                    if (etag == Etag.Empty)
                    {
                        var metadata = (RavenJObject) file["metadata"];

                        if (metadata.ContainsKey(RavenConstants.RavenDeleteMarker) &&
                            metadata.Value<bool>(RavenConstants.RavenDeleteMarker))
                        {
                            return existingEtag;
                        }
                    }

                    throw new ConcurrencyException("Operation attempted on file '" + key +
                                                   "' using a non current etag")
                    {
                        ActualETag = existingEtag,
                        ExpectedETag = etag
                    };
                }
            }

            return existingEtag;
        }
    }
}
