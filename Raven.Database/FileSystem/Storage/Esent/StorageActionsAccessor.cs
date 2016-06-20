//-----------------------------------------------------------------------
// <copyright file="StorageActionsAccessor.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

using Microsoft.Isam.Esent.Interop;


using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Abstractions.Util;
using Raven.Database.FileSystem.Infrastructure;
using Raven.Database.FileSystem.Plugins;
using Raven.Database.FileSystem.Storage.Exceptions;
using Raven.Database.FileSystem.Synchronization.Rdc;
using Raven.Database.FileSystem.Util;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Data;

namespace Raven.Database.FileSystem.Storage.Esent
{
    public class StorageActionsAccessor : IStorageActionsAccessor
    {
        private static ILog log = LogManager.GetCurrentClassLogger();
        private readonly JET_DBID database;
        private readonly Session session;
        private readonly TableColumnsCache tableColumnsCache;
        private readonly UuidGenerator uuidGenerator;
        private readonly OrderedPartCollection<AbstractFileCodec> fileCodecs;
        private Table config;
        private Table details;

        private Table files;
        private Table pages;
        private Table signatures;
        private Transaction transaction;
        private Table usage;

        public StorageActionsAccessor(TableColumnsCache tableColumnsCache, JET_INSTANCE instance, string databaseName, UuidGenerator uuidGenerator, OrderedPartCollection<AbstractFileCodec> fileCodecs)
        {
            this.tableColumnsCache = tableColumnsCache;
            this.uuidGenerator = uuidGenerator;
            this.fileCodecs = fileCodecs;
            try
            {
                session = new Session(instance);
                transaction = new Transaction(session);
                Api.JetOpenDatabase(session, databaseName, null, out database, OpenDatabaseGrbit.None);
            }
            catch (Exception original)
            {
                log.WarnException("Could not create accessor", original);
                try
                {
                    Dispose();
                }
                catch (Exception e)
                {
                    log.WarnException("Could not properly dispose accessor after exception in ctor.", e);
                }
                throw;
            }
        }

        private Table Files
        {
            get { return files ?? (files = new Table(session, database, "files", OpenTableGrbit.None)); }
        }

        private Table Signatures
        {
            get { return signatures ?? (signatures = new Table(session, database, "signatures", OpenTableGrbit.None)); }
        }

        private Table Config
        {
            get { return config ?? (config = new Table(session, database, "config", OpenTableGrbit.None)); }
        }

        private Table Usage
        {
            get { return usage ?? (usage = new Table(session, database, "usage", OpenTableGrbit.None)); }
        }

        private Table Pages
        {
            get { return pages ?? (pages = new Table(session, database, "pages", OpenTableGrbit.None)); }
        }

        private Table Details
        {
            get { return details ?? (details = new Table(session, database, "details", OpenTableGrbit.None)); }
        }


        [DebuggerHidden]
        [DebuggerNonUserCode]
        public void Dispose()
        {
            if (signatures != null)
                signatures.Dispose();
            if (config != null)
                config.Dispose();
            if (details != null)
                details.Dispose();
            if (pages != null)
                pages.Dispose();
            if (usage != null)
                usage.Dispose();
            if (files != null)
                files.Dispose();
            if (Equals(database, JET_DBID.Nil) == false && session != null)
                Api.JetCloseDatabase(session, database, CloseDatabaseGrbit.None);
            if (transaction != null)
                transaction.Dispose();
            if (session != null)
                session.Dispose();
        }

        [DebuggerHidden]
        [DebuggerNonUserCode]
        public void Commit()
        {
            transaction.Commit(CommitTransactionGrbit.None);
        }

        public void PulseTransaction()
        {
            transaction.Commit(CommitTransactionGrbit.LazyFlush);
            transaction.Dispose();
            transaction = new Transaction(session);
        }

        private static int bookmarkMost = SystemParameters.BookmarkMost;

        public int InsertPage(byte[] buffer, int size)
        {
            var key = new HashKey(buffer, size);

            Api.JetSetCurrentIndex(session, Pages, "by_keys");

            Api.MakeKey(session, Pages, key.Weak, MakeKeyGrbit.NewKey);
            Api.MakeKey(session, Pages, key.Strong, MakeKeyGrbit.None);

            if (Api.TrySeek(session, Pages, SeekGrbit.SeekEQ))
            {
                Api.EscrowUpdate(session, Pages, tableColumnsCache.PagesColumns["usage_count"], 1);
                return Api.RetrieveColumnAsInt32(session, Pages, tableColumnsCache.PagesColumns["id"]).Value;
            }

            var bookMarkBuffer = new byte[bookmarkMost];
            var actualSize = 0;
            using (var update = new Update(session, Pages, JET_prep.Insert))
            {
                Api.SetColumn(session, Pages, tableColumnsCache.PagesColumns["page_strong_hash"], key.Strong);
                Api.SetColumn(session, Pages, tableColumnsCache.PagesColumns["page_weak_hash"], key.Weak);

                using (var columnStream = new ColumnStream(session, Pages, tableColumnsCache.PagesColumns["data"]))
                {
                    using (Stream stream = new BufferedStream(columnStream))
                    using (var finalStream = fileCodecs.Aggregate(stream, (current, codec) => codec.EncodePage(current)))
                    {
                        finalStream.Write(buffer, 0, size);
                        finalStream.Flush();
                    }
                }

                try
                {
                    update.Save(bookMarkBuffer, bookMarkBuffer.Length, out actualSize);
                }
                catch (EsentKeyDuplicateException)
                {
                    // it means that page is being inserted by another thread
                    throw new ConcurrencyException("The same file page is being created");
                }
            }

            Api.JetGotoBookmark(session, Pages, bookMarkBuffer, actualSize);

            return Api.RetrieveColumnAsInt32(session, Pages, tableColumnsCache.PagesColumns["id"]).Value;
        }

        public FileUpdateResult PutFile(string filename, long? totalSize, RavenJObject metadata, bool tombstone = false)
        {
            FileUpdateResult result;

            using (var update = new Update(session, Files, JET_prep.Insert))
            {
                Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["name"], filename, Encoding.Unicode);
                if (totalSize != null)
                    Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["total_size"], BitConverter.GetBytes(totalSize.Value));

                Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["uploaded_size"], BitConverter.GetBytes(0));

                metadata.Remove(Constants.MetadataEtagField);
                var newEtag = uuidGenerator.CreateSequentialUuid();

                Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["etag"], newEtag.TransformToValueForEsentSorting());
                Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["metadata"], ToQueryString(metadata), Encoding.Unicode);

                update.Save();

                result = new FileUpdateResult
                {
                    PrevEtag = null,
                    Etag = newEtag
                };
            }

            if (!tombstone)
            {
                if (Api.TryMoveFirst(session, Details) == false)
                    throw new InvalidOperationException("Could not find system metadata row");

                Api.EscrowUpdate(session, Details, tableColumnsCache.DetailsColumns["file_count"], 1);
            }

            return result;
        }

        private static string ToQueryString(RavenJObject metadata)
        {
            var serializer = JsonExtensions.CreateDefaultJsonSerializer();
            var sb = new StringBuilder();
            serializer.Serialize(new JsonTextWriter(new StringWriter(sb)), metadata);

            return sb.ToString();
        }

        public void AssociatePage(string filename, int pageId, int pagePositionInFile, int pageSize)
        {
            Api.JetSetCurrentIndex(session, Files, "by_name");
            Api.MakeKey(session, Files, filename, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Files, SeekGrbit.SeekEQ) == false)
                throw new FileNotFoundException("Could not find file: " + filename);

            using (var update = new Update(session, Files, JET_prep.Replace))
            {
                var totalSize = GetTotalSize();
                var uploadedSize =
                    BitConverter.ToInt64(Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["uploaded_size"]), 0);

                if (totalSize != null && totalSize >= 0 && uploadedSize + pageSize > totalSize)
                    throw new InvalidDataException("Try to upload more data than the file was allocated for (" + totalSize +
                                                   ") and new size would be: " + (uploadedSize + pageSize));

                Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["uploaded_size"],
                              BitConverter.GetBytes(uploadedSize + pageSize));

                // using chunked encoding, we don't know what the size is
                // we use negative values here for keeping track of the unknown size
                if (totalSize == null || totalSize < 0)
                {
                    var actualSize = totalSize ?? 0;
                    Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["total_size"],
                                  BitConverter.GetBytes(actualSize - pageSize));
                }

                update.Save();
            }

            using (var update = new Update(session, Usage, JET_prep.Insert))
            {
                Api.SetColumn(session, Usage, tableColumnsCache.UsageColumns["name"], filename, Encoding.Unicode);
                Api.SetColumn(session, Usage, tableColumnsCache.UsageColumns["file_pos"], pagePositionInFile);
                Api.SetColumn(session, Usage, tableColumnsCache.UsageColumns["page_id"], pageId);
                Api.SetColumn(session, Usage, tableColumnsCache.UsageColumns["page_size"], pageSize);

                update.Save();
            }
        }

        private long? GetTotalSize()
        {
            var totalSize = Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["total_size"]);
            if (totalSize == null)
                return null;

            return BitConverter.ToInt64(totalSize, 0);
        }

        public int ReadPage(int pageId, byte[] buffer)
        {
            Api.JetSetCurrentIndex(session, Pages, "by_id");
            Api.MakeKey(session, Pages, pageId, MakeKeyGrbit.NewKey);

            if (Api.TrySeek(session, Pages, SeekGrbit.SeekEQ) == false)
                return -1;

            using (Stream stream = new BufferedStream(new ColumnStream(session, Pages, tableColumnsCache.PagesColumns["data"])))
            {
                using (var decodedStream = fileCodecs.Aggregate(stream, (bytes, codec) => codec.DecodePage(bytes)))
                {
                    return decodedStream.Read(buffer, 0, buffer.Length);
                }
            }
        }

        public FileHeader ReadFile(string filename)
        {
            Api.JetSetCurrentIndex(session, Files, "by_name");
            Api.MakeKey(session, Files, filename, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Files, SeekGrbit.SeekEQ) == false)
                return null;

            return new FileHeader ( Api.RetrieveColumnAsString(session, Files, tableColumnsCache.FilesColumns["name"], Encoding.Unicode), RetrieveMetadata())
                       {
                           TotalSize = GetTotalSize(),
                           UploadedSize = BitConverter.ToInt64(Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["uploaded_size"]), 0),
                       };
        }

        public FileAndPagesInformation GetFile(string filename, int start, int pagesToLoad)
        {
            Api.JetSetCurrentIndex(session, Files, "by_name");
            Api.MakeKey(session, Files, filename, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Files, SeekGrbit.SeekEQ) == false)
                throw new FileNotFoundException("Could not find file: " + filename);

            var fileInformation = new FileAndPagesInformation
                                      {
                                          TotalSize = GetTotalSize(),
                                          UploadedSize = BitConverter.ToInt64(Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["uploaded_size"]), 0),
                                          Metadata = RetrieveMetadata(),
                                          Name = Api.RetrieveColumnAsString(session, Files, tableColumnsCache.FilesColumns["name"]),
                                          Start = start
                                      };

            if (pagesToLoad > 0)
            {
                Api.JetSetCurrentIndex(session, Usage, "by_name_and_pos");
                Api.MakeKey(session, Usage, filename, Encoding.Unicode, MakeKeyGrbit.NewKey);
                Api.MakeKey(session, Usage, start, MakeKeyGrbit.None);
                if (Api.TrySeek(session, Usage, SeekGrbit.SeekGE))
                {
                    Api.MakeKey(session, Usage, filename, Encoding.Unicode, MakeKeyGrbit.NewKey);
                    Api.JetSetIndexRange(session, Usage, SetIndexRangeGrbit.RangeInclusive);

                    do
                    {
                        var name = Api.RetrieveColumnAsString(session, Usage, tableColumnsCache.UsageColumns["name"]);
                        if (name.Equals(filename, StringComparison.InvariantCultureIgnoreCase) == false)
                            continue;

                        fileInformation.Pages.Add(new PageInformation
                                                      {
                                                          Size = Api.RetrieveColumnAsInt32(session, Usage, tableColumnsCache.UsageColumns["page_size"]).Value,
                                                          Id = Api.RetrieveColumnAsInt32(session, Usage, tableColumnsCache.UsageColumns["page_id"]).Value,
                                                          PositionInFile = Api.RetrieveColumnAsInt32(session, Usage, tableColumnsCache.UsageColumns["file_pos"]).Value
                                                      });
                    } while (Api.TryMoveNext(session, Usage) && fileInformation.Pages.Count < pagesToLoad);
                }
            }

            return fileInformation;
        }

        public IEnumerable<FileHeader> ReadFiles(int start, int size)
        {
            Api.JetSetCurrentIndex(session, Files, "by_name");
            if (Api.TryMoveFirst(session, Files) == false)
                yield break;

            try
            {
                Api.JetMove(session, Files, start, MoveGrbit.None);
            }
            catch (EsentErrorException e)
            {
                if (e.Error == JET_err.NoCurrentRecord)
                    yield break;
                throw;
            }

            var index = 0;

            do
            {
                yield return new FileHeader(Api.RetrieveColumnAsString(session, Files, tableColumnsCache.FilesColumns["name"], Encoding.Unicode), RetrieveMetadata() )
                                 {
                                     TotalSize = GetTotalSize(),
                                     UploadedSize = BitConverter.ToInt64(Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["uploaded_size"]), 0),
                                 };
            } while (++index < size && Api.TryMoveNext(session, Files));
        }

        private RavenJObject RetrieveMetadata()
        {
            var metadataAsString = Api.RetrieveColumnAsString(session, Files, tableColumnsCache.FilesColumns["metadata"], Encoding.Unicode);

            var metadata = RavenJObject.Parse(metadataAsString);
            
            // To avoid useless handling of conversions for special headers we return the same type we stored.
            if (metadata.ContainsKey(Constants.LastModified))
                metadata[Constants.LastModified] = metadata.Value<DateTimeOffset>(Constants.LastModified);   

            metadata[Constants.MetadataEtagField] = Etag.Parse(Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["etag"])).ToString();
            
            return metadata;
        }

        public IEnumerable<FileHeader> GetFilesAfter(Etag etag, int take)
        {
            Api.JetSetCurrentIndex(session, Files, "by_etag");
            Api.MakeKey(session, Files, etag.TransformToValueForEsentSorting(), MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Files, SeekGrbit.SeekGT) == false)
                return Enumerable.Empty<FileHeader>();

            var result = new List<FileHeader>();
            var index = 0;

            do
            {
                result.Add(new FileHeader (Api.RetrieveColumnAsString(session, Files, tableColumnsCache.FilesColumns["name"], Encoding.Unicode), RetrieveMetadata() )
                               {
                                   TotalSize = GetTotalSize(),
                                   UploadedSize = BitConverter.ToInt64( Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["uploaded_size"]), 0),
                               });
            } while (++index < take && Api.TryMoveNext(session, Files));

            return result;
        }

        public IEnumerable<FileHeader> GetFilesStartingWith(string namePrefix, int start, int take)
        {
            Api.JetSetCurrentIndex(session, Files, "by_name");

            Api.MakeKey(session, Files, namePrefix, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Files, SeekGrbit.SeekGE) == false)
            {
                yield break;
            }

            Api.MakeKey(session, Files, namePrefix, Encoding.Unicode, MakeKeyGrbit.NewKey | MakeKeyGrbit.PartialColumnEndLimit);
            try
            {
                Api.JetMove(session, Files, start, MoveGrbit.MoveKeyNE);
            }
            catch (EsentNoCurrentRecordException)
            {
                yield break;
            }

            if (Api.TrySetIndexRange(session, Files, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit))
            {
                var fetchedCount = 0;

                do
                {
                    var file = new FileHeader(Api.RetrieveColumnAsString(session, Files, tableColumnsCache.FilesColumns["name"], Encoding.Unicode), RetrieveMetadata())
                                 {
                                     TotalSize = GetTotalSize(), 
                                     UploadedSize = BitConverter.ToInt64(Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["uploaded_size"]), 0),
                                 };

                    if (file.FullPath.StartsWith(namePrefix) == false)
                        continue;

                    fetchedCount++;

                    yield return file;
                }
                while (Api.TryMoveNext(session, Files) && fetchedCount < take);
            }
        }

        public Etag GetLastEtag()
        {
            Api.JetSetCurrentIndex(session, Files, "by_etag");
            if (Api.TryMoveLast(session, Files) == false)
                return Etag.Empty;

            var val = Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["etag"], RetrieveColumnGrbit.RetrieveFromIndex, null);
            return Etag.Parse(val);
        }

        public bool IsNested { get; set; }

        public void Delete(string filename)
        {
            Api.JetSetCurrentIndex(session, Usage, "by_name_and_pos");
            Api.MakeKey(session, Usage, filename, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Usage, SeekGrbit.SeekGE))
            {
                Api.JetSetCurrentIndex(session, Pages, "by_id");

                var count = 0;
                do
                {
                    var rowName = Api.RetrieveColumnAsString(session, Usage, tableColumnsCache.UsageColumns["name"]);
                    if (rowName.Equals(filename, StringComparison.InvariantCultureIgnoreCase) == false)
                        break;

                    var pageId = Api.RetrieveColumnAsInt32(session, Usage, tableColumnsCache.UsageColumns["page_id"]).Value;

                    Api.MakeKey(session, Pages, pageId, MakeKeyGrbit.NewKey);

                    if (Api.TrySeek(session, Pages, SeekGrbit.SeekEQ))
                    {
                        var escrowUpdate = Api.EscrowUpdate(session, Pages, tableColumnsCache.PagesColumns["usage_count"], -1);
                        if (escrowUpdate <= 1)
                        {
                            Api.JetDelete(session, Pages);
                        }
                    }

                    Api.JetDelete(session, Usage);

                    if (count++ > 1000)
                    {
                        PulseTransaction();
                        count = 0;
                    }
                } while (Api.TryMoveNext(session, Usage));
            }

            Api.JetSetCurrentIndex(session, Files, "by_name");
            Api.MakeKey(session, Files, filename, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Files, SeekGrbit.SeekEQ) == false)
                return;

            Api.JetDelete(session, Files);
        }

        public FileUpdateResult UpdateFileMetadata(string filename, RavenJObject metadata, Etag etag)
        {
            Api.JetSetCurrentIndex(session, Files, "by_name");
            Api.MakeKey(session, Files, filename, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Files, SeekGrbit.SeekEQ) == false)
                throw new FileNotFoundException(filename);

            using (var update = new Update(session, Files, JET_prep.Replace))
            {
                var existingEtag = EnsureFileEtagMatch(filename, etag);
                metadata.Remove(Constants.MetadataEtagField);

                var existingMetadata = RetrieveMetadata();

                if (!metadata.ContainsKey("Content-MD5") && existingMetadata.ContainsKey("Content-MD5"))
                    metadata["Content-MD5"] = existingMetadata["Content-MD5"];

                if (!metadata.ContainsKey(Constants.FileSystem.RavenFsSize) && existingMetadata.ContainsKey(Constants.FileSystem.RavenFsSize))
                    metadata[Constants.FileSystem.RavenFsSize] = existingMetadata[Constants.FileSystem.RavenFsSize];

                var newEtag = uuidGenerator.CreateSequentialUuid();

                Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["etag"], newEtag.TransformToValueForEsentSorting());
                Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["metadata"], ToQueryString(metadata), Encoding.Unicode);

                update.Save();

                return new FileUpdateResult
                {
                    PrevEtag = existingEtag,
                    Etag = newEtag
                };
            }
        }

        public void CompleteFileUpload(string filename)
        {
            Api.JetSetCurrentIndex(session, Files, "by_name");
            Api.MakeKey(session, Files, filename, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Files, SeekGrbit.SeekEQ) == false)
                throw new FileNotFoundException("Could not find file: " + filename);

            using (var update = new Update(session, Files, JET_prep.Replace))
            {
                var totalSize = GetTotalSize() ?? 0;
                Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["total_size"],
                              BitConverter.GetBytes(Math.Abs(totalSize)));

                update.Save();
            }
        }

        public int GetFileCount()
        {
            if (Api.TryMoveFirst(session, Details) == false)
                throw new InvalidOperationException("Could not find system metadata row");

            return Api.RetrieveColumnAsInt32(session, Details, tableColumnsCache.DetailsColumns["file_count"]).Value;
        }

        public void DecrementFileCount(string nameOfFileThatShouldNotBeCounted)
        {
            if (Api.TryMoveFirst(session, Details) == false)
                throw new InvalidOperationException("Could not find system metadata row");

            Api.EscrowUpdate(session, Details, tableColumnsCache.DetailsColumns["file_count"], -1);
        }

        public void RenameFile(string filename, string rename, bool commitPeriodically = false)
        {
            try
            {
                Api.JetSetCurrentIndex(session, Files, "by_name");
                Api.MakeKey(session, Files, filename, Encoding.Unicode, MakeKeyGrbit.NewKey);
                if (Api.TrySeek(session, Files, SeekGrbit.SeekEQ) == false)
                    throw new FileNotFoundException("Could not find file: " + filename);

                using (var update = new Update(session, Files, JET_prep.Replace))
                {
                    Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["name"], rename, Encoding.Unicode);

                    update.Save();
                }

                Api.JetSetCurrentIndex(session, Usage, "by_name_and_pos");
                Api.MakeKey(session, Usage, filename, Encoding.Unicode, MakeKeyGrbit.NewKey);
                if (Api.TrySeek(session, Usage, SeekGrbit.SeekGE))
                {
                    var count = 0;
                    do
                    {
                        var name = Api.RetrieveColumnAsString(session, Usage, tableColumnsCache.UsageColumns["name"]);
                        if (name.Equals(filename, StringComparison.InvariantCultureIgnoreCase) == false)
                            break;

                        using (var update = new Update(session, Usage, JET_prep.Replace))
                        {
                            Api.SetColumn(session, Usage, tableColumnsCache.UsageColumns["name"], rename, Encoding.Unicode);

                            update.Save();
                        }

                        if (commitPeriodically && count++ > 1000)
                        {
                            PulseTransaction();
                            count = 0;
                        }
                    } while (Api.TryMoveNext(session, Usage));
                }
            }
            catch (Exception e)
            {
                if (e is EsentKeyDuplicateException)
                    throw new FileExistsException(string.Format("Cannot rename '{0}' to '{1}'. File '{1}' already exists.", filename, rename), e);

                throw;
            }
        }

        public void CopyFile(string sourceFilename, string targetFilename, bool commitPeriodically = false)
        {
            try
            {
                var sourceFileInfo = GetFile(sourceFilename, 0, int.MaxValue);

                using (var update = new Update(session, Files, JET_prep.Insert))
                {
                    Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["name"], targetFilename, Encoding.Unicode);
                    if (sourceFileInfo.TotalSize != null)
                        Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["total_size"], BitConverter.GetBytes(sourceFileInfo.TotalSize.Value));

                    Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["uploaded_size"], sourceFileInfo.UploadedSize);

                    sourceFileInfo.Metadata.Remove(Constants.MetadataEtagField);
                    var newEtag = uuidGenerator.CreateSequentialUuid();

                    Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["etag"], newEtag.TransformToValueForEsentSorting());
                    Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["metadata"], ToQueryString(sourceFileInfo.Metadata), Encoding.Unicode);

                    update.Save();
                }
                
                var count = 0;
                foreach (var pageInfo in sourceFileInfo.Pages)
                {
                    using (var update = new Update(session, Usage, JET_prep.Insert))
                    {
                        Api.SetColumn(session, Usage, tableColumnsCache.UsageColumns["name"], targetFilename, Encoding.Unicode);
                        Api.SetColumn(session, Usage, tableColumnsCache.UsageColumns["file_pos"], pageInfo.PositionInFile);
                        Api.SetColumn(session, Usage, tableColumnsCache.UsageColumns["page_id"], pageInfo.Id);
                        Api.SetColumn(session, Usage, tableColumnsCache.UsageColumns["page_size"], pageInfo.Size);

                        update.Save();
                    }

                    if (commitPeriodically && count++ > 1000)
                    {
                        PulseTransaction();
                        count = 0;
                    }
                }
            }
            catch (Exception e)
            {
                if (e is EsentKeyDuplicateException)
                    throw new FileExistsException(string.Format("Cannot copy '{0}' to '{1}'. File '{1}' already exists.", sourceFilename, targetFilename), e);

                throw;
            }
        }

        public RavenJObject GetConfig(string name)
        {
            Api.JetSetCurrentIndex(session, Config, "by_name");
            Api.MakeKey(session, Config, name, Encoding.Unicode, MakeKeyGrbit.NewKey);			
            if (Api.TrySeek(session, Config, SeekGrbit.SeekEQ) == false)
                throw new FileNotFoundException("Could not find config: " + name);
            
            var metadata = Api.RetrieveColumnAsString(session, Config, tableColumnsCache.ConfigColumns["metadata"], Encoding.Unicode);
            return RavenJObject.Parse(metadata);
        }

        public void SetConfig(string name, RavenJObject metadata)
        {
            var builder = new StringBuilder();
            using (var writer = new JsonTextWriter(new StringWriter(builder)))
                metadata.WriteTo(writer);

            string metadataString = builder.ToString();            
            
            Api.JetSetCurrentIndex(session, Config, "by_name");
            Api.MakeKey(session, Config, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
            var prep = Api.TrySeek(session, Config, SeekGrbit.SeekEQ) ? JET_prep.Replace : JET_prep.Insert;

            using (var update = new Update(session, Config, prep))            
            {
                Api.SetColumn(session, Config, tableColumnsCache.ConfigColumns["name"], name, Encoding.Unicode);
                Api.SetColumn(session, Config, tableColumnsCache.ConfigColumns["metadata"], metadataString, Encoding.Unicode);

                update.Save();
            }
        }

        public void DeleteConfig(string name)
        {
            Api.JetSetCurrentIndex(session, Config, "by_name");
            Api.MakeKey(session, Config, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Config, SeekGrbit.SeekEQ) == false)
                return;

            Api.JetDelete(session, Config);
        }

        public IEnumerable<SignatureLevels> GetSignatures(string name)
        {
            Api.JetSetCurrentIndex(session, Signatures, "by_name");
            Api.MakeKey(session, Signatures, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Signatures, SeekGrbit.SeekEQ) == false)
                yield break;

            Api.MakeKey(session, Signatures, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
            Api.TrySetIndexRange(session, Signatures, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);

            do
            {
                yield return new SignatureLevels
                                 {
                                     Id = Api.RetrieveColumnAsInt32(session, Signatures, tableColumnsCache.SignaturesColumns["id"]).Value,
                                     Level =
                                         Api.RetrieveColumnAsInt32(session, Signatures, tableColumnsCache.SignaturesColumns["level"]).Value,
                                     CreatedAt =
                                         Api.RetrieveColumnAsDateTime(session, Signatures, tableColumnsCache.SignaturesColumns["created_at"])
                                            .Value
                                 };
            } while (Api.TryMoveNext(session, Signatures));
        }

        public void ClearSignatures(string name)
        {
            Api.JetSetCurrentIndex(session, Signatures, "by_name");
            Api.MakeKey(session, Signatures, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Signatures, SeekGrbit.SeekEQ) == false)
                return;

            Api.MakeKey(session, Signatures, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
            Api.TrySetIndexRange(session, Signatures, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);

            do
            {
                Api.JetDelete(session, Signatures);
            } while (Api.TryMoveNext(session, Signatures));
        }


        public long GetSignatureSize(int id, int level)
        {
            Api.JetSetCurrentIndex(session, Signatures, "by_id");
            Api.MakeKey(session, Signatures, id, MakeKeyGrbit.NewKey);
            Api.MakeKey(session, Signatures, level, MakeKeyGrbit.None);
            if (Api.TrySeek(session, Signatures, SeekGrbit.SeekEQ) == false)
                throw new InvalidOperationException("Could not find signature with id " + id + " and level " + level);

            return Api.RetrieveColumnSize(session, Signatures, tableColumnsCache.SignaturesColumns["data"]) ?? 0;
        }

        public void GetSignatureStream(int id, int level, Action<Stream> action)
        {
            Api.JetSetCurrentIndex(session, Signatures, "by_id");
            Api.MakeKey(session, Signatures, id, MakeKeyGrbit.NewKey);
            Api.MakeKey(session, Signatures, level, MakeKeyGrbit.None);
            if (Api.TrySeek(session, Signatures, SeekGrbit.SeekEQ) == false)
                throw new InvalidOperationException("Could not find signature with id " + id + " and level " + level);


            using (var stream = new ColumnStream(session, Signatures, tableColumnsCache.SignaturesColumns["data"]))
            using (var buffer = new BufferedStream(stream))
            {
                action(buffer);
                buffer.Flush();
                stream.Flush();
            }
        }

        public void AddSignature(string name, int level, Action<Stream> action)
        {
            using (var update = new Update(session, Signatures, JET_prep.Insert))
            {
                Api.SetColumn(session, Signatures, tableColumnsCache.SignaturesColumns["name"], name, Encoding.Unicode);
                Api.SetColumn(session, Signatures, tableColumnsCache.SignaturesColumns["level"], level);
                Api.SetColumn(session, Signatures, tableColumnsCache.SignaturesColumns["created_at"], DateTime.UtcNow);

                using (var stream = new ColumnStream(session, Signatures, tableColumnsCache.SignaturesColumns["data"]))
                using (var buffer = new BufferedStream(stream))
                {
                    action(buffer);
                    buffer.Flush();
                    stream.Flush();
                }

                update.Save();
            }
        }

        public IEnumerable<string> GetConfigNames(int start, int pageSize)
        {
            Api.JetSetCurrentIndex(session, Config, "by_name");
            Api.MoveBeforeFirst(session, Config);
            for (var i = 0; i < start; i++)
            {
                if (Api.TryMoveNext(session, Config) == false)
                    yield break;
            }

            int count = 0;
            while (Api.TryMoveNext(session, Config) && count < pageSize)
            {
                yield return Api.RetrieveColumnAsString(session, Config, tableColumnsCache.ConfigColumns["name"]);
                count++;
            }
        }

        public bool ConfigExists(string name)
        {
            Api.JetSetCurrentIndex(session, Config, "by_name");
            Api.MakeKey(session, Config, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
            return Api.TrySeek(session, Config, SeekGrbit.SeekEQ);
        }

        public IList<RavenJObject> GetConfigsStartWithPrefix(string prefix, int start, int take, out int totalCount)
        {
            var configs = new List<RavenJObject>();

            var totalCountRef = new Reference<int>();

            using (var enumerator = IterateConfigsWithPrefix(prefix, start, take, totalCountRef).GetEnumerator())
            {
                while (enumerator.MoveNext())
                {
                    var metadata = Api.RetrieveColumnAsString(session, Config, tableColumnsCache.ConfigColumns["metadata"], Encoding.Unicode);
                    configs.Add(RavenJObject.Parse(metadata));
                }
            }

            totalCount = totalCountRef.Value;

            return configs;
        }

        public IList<string> GetConfigNamesStartingWithPrefix(string prefix, int start, int take, out int total)
        {
            var configs = new List<string>();
            var totalCountRef = new Reference<int>();

            using (var enumerator = IterateConfigsWithPrefix(prefix, start, take, totalCountRef).GetEnumerator())
            {
                while (enumerator.MoveNext())
                {
                    var configName = Api.RetrieveColumnAsString(session, Config, tableColumnsCache.ConfigColumns["name"]);
                    configs.Add(configName);
                }
            }

            total = totalCountRef.Value;

            return configs;
        }

        private IEnumerable<object> IterateConfigsWithPrefix(string prefix, int start, int take, Reference<int> totalCount)
        {
            Api.JetSetCurrentIndex(session, Config, "by_name");

            Api.MakeKey(session, Config, prefix, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Config, SeekGrbit.SeekGE) == false)
            {
                totalCount.Value = 0;
                yield break;
            }

            Api.MakeKey(session, Config, prefix, Encoding.Unicode, MakeKeyGrbit.NewKey | MakeKeyGrbit.PartialColumnEndLimit);
            Api.TrySetIndexRange(session, Config, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);

            try
            {
                Api.JetMove(session, Config, start, MoveGrbit.MoveKeyNE);
            }
            catch (EsentNoCurrentRecordException)
            {
                // looks like we requested start higher then amount of available objects
                // however we have to provide total count
                // restart index and compute total count
                Api.MakeKey(session, Config, prefix, Encoding.Unicode, MakeKeyGrbit.NewKey);
                Api.TrySeek(session, Config, SeekGrbit.SeekGE);

                Api.MakeKey(session, Config, prefix, Encoding.Unicode,
                       MakeKeyGrbit.NewKey | MakeKeyGrbit.PartialColumnEndLimit);

                totalCount.Value = 0;

                if (Api.TrySetIndexRange(session, Config,
                    SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit))
                {
                    do
                    {
                        totalCount.Value++;
                    } while (Api.TryMoveNext(session, Config));
                }

                yield break;
            }

            bool hasNextRecord;
            var returned = 0;
            var enumerate = new object();

            do
            {
                yield return enumerate;
                returned++;
                hasNextRecord = Api.TryMoveNext(session, Config);
            } while (hasNextRecord && returned < take);

            var extraRecords = 0;
            if (hasNextRecord)
            {
                Api.JetIndexRecordCount(session, Config, out extraRecords, 0);
            }

            totalCount.Value = start + returned + extraRecords;
        }

        private Etag EnsureFileEtagMatch(string key, Etag etag)
        {
            var existingEtag = Etag.Parse(Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["etag"]));

            if (etag != null)
            {
                if (existingEtag != etag)
                {
                    if (etag == Etag.Empty)
                    {
                        var metadata = RavenJObject.Parse(Api.RetrieveColumnAsString(session, Files, tableColumnsCache.FilesColumns["metadata"], Encoding.Unicode));

                        if (metadata.ContainsKey(Constants.RavenDeleteMarker) &&
                            metadata.Value<bool>(Constants.RavenDeleteMarker))
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
