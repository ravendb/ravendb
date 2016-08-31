//-----------------------------------------------------------------------
// <copyright file="Documents.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Bundles.Compression.Plugin;
using Raven.Json.Linq;
using Raven.Storage.Esent.StorageActions;

namespace Raven.Database.Storage.Esent.StorageActions
{
    public partial class DocumentStorageActions : IDocumentStorageActions
    {
        [ThreadStatic]
        private static byte[] readBuffer;

        public long GetDocumentsCount()
        {
            if (Api.TryMoveFirst(session, Details))
                return Api.RetrieveColumnAsInt32(session, Details, tableColumnsCache.DetailsColumns["document_count"]).Value;
            return 0;
        }

        public JsonDocument DocumentByKey(string key)
        {
            return DocumentByKeyInternal(key, (metadata, metadataSize, createDocument) =>
            {
                System.Diagnostics.Debug.Assert(metadata.Etag != null);

                var docSize = new Reference<int>();
                var doc = createDocument(metadata.Key, metadata.Etag, metadata.Metadata, docSize);

                var serializedSizeOnDisk = metadataSize + docSize.Value;
                cacher.SetCachedDocument(key, metadata.Etag, doc, metadata.Metadata, serializedSizeOnDisk);

                return new JsonDocument
                {
                    DataAsJson = doc,
                    Etag = metadata.Etag,
                    Key = metadata.Key,
                    LastModified = metadata.LastModified,
                    Metadata = metadata.Metadata,
                    SerializedSizeOnDisk = serializedSizeOnDisk,
                    NonAuthoritativeInformation = metadata.NonAuthoritativeInformation
                };
            });
        }

        public Stream RawDocumentByKey(string key)
        {
            Api.JetSetCurrentIndex(session, Documents, "by_key");
            Api.MakeKey(session, Documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Documents, SeekGrbit.SeekEQ) == false)
            {
                if (logger.IsDebugEnabled)
                    logger.Debug("Document with key '{0}' was not found", key);
                return null;
            }

            return new BufferedStream(new ColumnStream(session, Documents, tableColumnsCache.DocumentsColumns["data"]));
        }

        public JsonDocumentMetadata DocumentMetadataByKey(string key)
        {
            return DocumentByKeyInternal(key, (metadata, metadataSize, func) => metadata);
        }

        private T DocumentByKeyInternal<T>(string key, Func<JsonDocumentMetadata, int, Func<string, Etag, RavenJObject, Reference<int>, RavenJObject>, T> createResult)
            where T : class
        {
            Api.JetSetCurrentIndex(session, Documents, "by_key");
            Api.MakeKey(session, Documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Documents, SeekGrbit.SeekEQ) == false)
            {
                if (logger.IsDebugEnabled)
                    logger.Debug("Document with key '{0}' was not found", key);
                return null;
            }

            var existingEtag = Etag.Parse(Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"]));
            if (logger.IsDebugEnabled)
                logger.Debug("Document with key '{0}' was found, etag: {1}", key, existingEtag);

            var lastModifiedInt64 = Api.RetrieveColumnAsInt64(session, Documents, tableColumnsCache.DocumentsColumns["last_modified"]).Value;

            int metadataSize;
            var metadata = ReadDocumentMetadata(key, existingEtag, out metadataSize);
            return createResult(new JsonDocumentMetadata
            {
                Etag = existingEtag,
                LastModified = DateTime.FromBinary(lastModifiedInt64),
                Key = Api.RetrieveColumnAsString(session, Documents, tableColumnsCache.DocumentsColumns["key"], Encoding.Unicode),
                Metadata = metadata
            }, metadataSize, ReadDocumentData);
        }

        private RavenJObject ReadDocumentMetadata(string key, Etag existingEtag, out int size)
        {
            try
            {
                var existingCachedDocument = cacher.GetCachedDocument(key, existingEtag);
                if (existingCachedDocument != null)
                {
                    size = Api.RetrieveColumnSize(session, Documents, tableColumnsCache.DocumentsColumns["metadata"]) ?? 0;
                    return existingCachedDocument.Metadata;
                }

                RavenJObject metadata;
                size = GetMetadataFromStorage(out metadata);
                return metadata;
            }
            catch (Exception e)
            {
                throw new InvalidDataException("Failed to de-serialize metadata of document " + key, e);
            }
        }

        private RavenJObject ReadDocumentData(string key, Etag existingEtag, RavenJObject metadata, Reference<int> docSize)
        {
            try
            {
                var existingCachedDocument = cacher.GetCachedDocument(key, existingEtag);
                if (existingCachedDocument != null)
                {
                    docSize.Value = existingCachedDocument.Size;
                    return existingCachedDocument.Document;
                }

                RavenJObject document;
                docSize.Value = GetDocumentFromStorage(key, metadata, existingEtag, out document);
                return document;
            }
            catch (Exception e)
            {
                InvalidDataException invalidDataException = null;
                try
                {
                    using (Stream stream = new BufferedStream(new ColumnStream(session, Documents, tableColumnsCache.DocumentsColumns["data"])))
                    using (var reader = new BinaryReader(stream))
                    {
                        if (reader.ReadUInt32() == DocumentCompression.CompressFileMagic)
                        {
                            invalidDataException = new InvalidDataException(string.Format("Document '{0}' is compressed, but the compression bundle is not enabled.\r\n" +
                                                                                          "You have to enable the compression bundle when dealing with compressed documents.", key), e);
                        }
                    }
                }
                catch (Exception)
                {
                    // we are already in error handling mode, just ignore this
                }
                if (invalidDataException != null)
                    throw invalidDataException;

                throw new InvalidDataException("Failed to de-serialize a document: " + key, e);
            }
        }

        public IEnumerable<JsonDocument> GetDocumentsByReverseUpdateOrder(int start, int take)
        {
            Api.JetSetCurrentIndex(session, Documents, "by_etag");
            Api.MoveAfterLast(session, Documents);
            if (TryMoveTableRecords(Documents, start, backward: true))
                return Enumerable.Empty<JsonDocument>();
            if (take < 1024 * 4)
            {
                var optimizer = new OptimizedIndexReader();
                while (Api.TryMovePrevious(session, Documents) && optimizer.Count < take)
                {
                    optimizer.Add(Session, Documents);
                }

                return optimizer.Select(Session, Documents, ReadCurrentDocument);
            }
            return GetDocumentsWithoutBuffering(take);
        }

        private IEnumerable<JsonDocument> GetDocumentsWithoutBuffering(int take)
        {
            while (Api.TryMovePrevious(session, Documents) && take >= 0)
            {
                take--;
                yield return ReadCurrentDocument();
            }
        }

        private bool TryMoveTableRecords(Table table, int start, bool backward)
        {
            if (start <= 0)
                return false;
            if (start == int.MaxValue)
                return true;
            if (backward)
                start *= -1;
            try
            {
                Api.JetMove(session, table, start, MoveGrbit.None);
            }
            catch (EsentErrorException e)
            {
                if (e.Error == JET_err.NoCurrentRecord)
                {
                    return true;
                }
                throw;
            }
            return false;
        }

        private JsonDocument ReadCurrentDocument(string key)
        {
            var existingEtag = Etag.Parse(Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"]));
            var lastModified = Api.RetrieveColumnAsInt64(session, Documents, tableColumnsCache.DocumentsColumns["last_modified"]).Value;

            var existingCachedDocument = cacher.GetCachedDocument(key, existingEtag);
            if (existingCachedDocument != null)
            {
                return new JsonDocument
                {
                    SerializedSizeOnDisk = existingCachedDocument.Size,
                    Key = key,
                    DataAsJson = existingCachedDocument.Document,
                    NonAuthoritativeInformation = false,
                    LastModified = DateTime.FromBinary(lastModified),
                    Etag = existingEtag,
                    Metadata = existingCachedDocument.Metadata
                };
            }


            RavenJObject metadata;
            var metadataSize = GetMetadataFromStorage(out metadata);

            RavenJObject dataAsJson;
            var docSize = GetDocumentFromStorage(key, metadata, existingEtag, out dataAsJson);

            return new JsonDocument
            {
                SerializedSizeOnDisk = metadataSize + docSize,
                Key = key,
                DataAsJson = dataAsJson,
                NonAuthoritativeInformation = false,
                LastModified = DateTime.FromBinary(lastModified),
                Etag = existingEtag,
                Metadata = metadata
            };
        }

        private int GetDocumentFromStorage(string key, RavenJObject metadata, Etag existingEtag, out RavenJObject dataAsJson)
        {
            var colSize = Api.RetrieveColumnSize(session, Documents, tableColumnsCache.DocumentsColumns["data"]) ?? 0;
            EnsureBuffer(colSize);

            Api.JetRetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["data"],
                readBuffer, colSize,
                0, out colSize, RetrieveColumnGrbit.None, null);

            int docSize;
            using (Stream stream = new MemoryStream(readBuffer, 0, colSize, writable: false))
            {
                docSize = (int) stream.Length;

                using (var columnStream = documentCodecs.Aggregate(stream, (dataStream, codec) => codec.Decode(key, metadata, dataStream)))
                {
                    dataAsJson = columnStream.ToJObject();
                }
            }

            return docSize;
        }

        private int GetMetadataFromStorage(out RavenJObject metadata)
        {
            var colSize = Api.RetrieveColumnSize(session, Documents, tableColumnsCache.DocumentsColumns["metadata"]) ?? 0;
            EnsureBuffer(colSize);

            Api.JetRetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["metadata"],
                readBuffer, colSize,
                0, out colSize, RetrieveColumnGrbit.None, null);

            int metadataSize;
            using (Stream stream = new MemoryStream(readBuffer, 0, colSize, writable: false))
            {
                metadata = stream.ToJObject();
                metadataSize = (int) stream.Length;
            }
            return metadataSize;
        }

        private static void EnsureBuffer(int colSize)
        {
            if (readBuffer == null || readBuffer.Length < colSize)
                readBuffer = new byte[colSize];
        }

        private JsonDocument ReadCurrentDocument()
        {
            var key = Api.RetrieveColumnAsString(session, Documents, tableColumnsCache.DocumentsColumns["key"], Encoding.Unicode);
            return ReadCurrentDocument(key);
        }

        public IEnumerable<JsonDocument> GetDocumentsAfterWithIdStartingWith(Etag etag, string idPrefix, int take, 
            CancellationToken cancellationToken, long? maxSize = null, Etag untilEtag = null, TimeSpan? timeout = null, 
            Action<Etag> lastProcessedDocument = null, Reference<bool> earlyExit = null, Action<List<DocumentFetchError>> failedToGetHandler = null)
        {
            if (earlyExit != null)
                earlyExit.Value = false;
            Api.JetSetCurrentIndex(session, Documents, "by_etag");
            Api.MakeKey(session, Documents, etag.TransformToValueForEsentSorting(), MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Documents, SeekGrbit.SeekGT) == false)
                yield break;

            long totalSize = 0;
            int fetchedDocumentCount = 0;

            Stopwatch duration = null;
            if (timeout != null)
                duration = Stopwatch.StartNew();

            Etag lastDocEtag = null;
            Etag docEtag = etag;

            var errors = new List<DocumentFetchError>();
            var skipDocumentGetErrors = failedToGetHandler != null;
            do
            {
                cancellationToken.ThrowIfCancellationRequested();
                           
                docEtag = Etag.Parse(Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"]));

                // We can skip many documents so the timeout should be at the start of the process to be executed.
                if (timeout != null)
                {
                    if (duration.Elapsed > timeout.Value)
                    {
                        if (earlyExit != null)
                            earlyExit.Value = true;
                        break;
                    }
                }                

                if (untilEtag != null && fetchedDocumentCount > 0)
                {
                    // This is not a failure, we are just ahead of when we expected to. 
                    if (EtagUtil.IsGreaterThan(docEtag, untilEtag))
                        break;
                }

                var key = Api.RetrieveColumnAsString(session, Documents, tableColumnsCache.DocumentsColumns["key"], Encoding.Unicode);
                if (!string.IsNullOrEmpty(idPrefix))
                {
                    if (!key.StartsWith(idPrefix, StringComparison.OrdinalIgnoreCase))
                    {
                        // We assume that we have processed it because it is not of our interest.
                        lastDocEtag = docEtag;
                        continue;
                    }                        
                }

                JsonDocument readCurrentDocument;
                try
                {
                    readCurrentDocument = ReadCurrentDocument(key);
                }
                catch (Exception e)
                {
                    if (skipDocumentGetErrors)
                    {
                        errors.Add(new DocumentFetchError
                        {
                            Key = key,
                            Exception = e
                        });
                        continue;
                    }

                    throw;
                }

                totalSize += readCurrentDocument.SerializedSizeOnDisk;
                fetchedDocumentCount++;

                yield return readCurrentDocument;
                lastDocEtag = docEtag;  

                if (maxSize != null && totalSize > maxSize.Value)
                {
                    if (untilEtag != null && earlyExit != null)
                        earlyExit.Value = true;
                    break;
                }

                if (fetchedDocumentCount >= take)
                {
                    if (untilEtag !=null && earlyExit != null)
                        earlyExit.Value = true;
                    break;
                }
            } 
            while (Api.TryMoveNext(session, Documents));

            if (skipDocumentGetErrors && errors.Count > 0)
            {
                failedToGetHandler(errors);
            }

            // We notify the last that we considered.
            if (lastProcessedDocument != null)
                lastProcessedDocument(lastDocEtag);
        }

        public IEnumerable<string> GetDocumentIdsAfterEtag(Etag etag, int maxTake,
            Func<string, RavenJObject, Func<JsonDocument>, bool> filterDocument, 
            Reference<bool> earlyExit, CancellationToken cancellationToken)
        {
            earlyExit.Value = false;

            Api.JetSetCurrentIndex(session, Documents, "by_etag");
            Api.MakeKey(session, Documents, etag.TransformToValueForEsentSorting(), MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Documents, SeekGrbit.SeekGT) == false)
                yield break;

            long fetchedDocumentCount = 0;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (++fetchedDocumentCount >= maxTake)
                {
                    earlyExit.Value = true;
                    break;
                }

                var key = Api.RetrieveColumnAsString(session, Documents, tableColumnsCache.DocumentsColumns["key"], Encoding.Unicode);

                RavenJObject metadata;
                GetMetadataFromStorage(out metadata);

                Func<JsonDocument> getDocument = () => DocumentByKey(key);
                if (filterDocument(key, metadata, getDocument) == false)
                    continue;

                yield return key;

            } while (Api.TryMoveNext(session, Documents));
        }

        public IEnumerable<JsonDocument> GetDocumentsAfter(Etag etag, int take,
            CancellationToken cancellationToken, long? maxSize = null, Etag untilEtag = null, TimeSpan? timeout = null, 
            Action<Etag> lastProcessedOnFailure = null, Reference<bool> earlyExit = null, Action<List<DocumentFetchError>> failedToGetHandler = null)
        {
            return GetDocumentsAfterWithIdStartingWith(etag, null, take, cancellationToken, maxSize, untilEtag, 
                timeout, lastProcessedOnFailure, earlyExit, failedToGetHandler);
        }

        public Etag GetBestNextDocumentEtag(Etag etag)
        {
            Api.JetSetCurrentIndex(session, Documents, "by_etag");
            Api.MakeKey(session, Documents, etag.TransformToValueForEsentSorting(), MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Documents, SeekGrbit.SeekGT) == false)
                return etag;


            var val = Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"],
                                         RetrieveColumnGrbit.RetrieveFromIndex, null);
            return Etag.Parse(val);
        }

        public DebugDocumentStats GetDocumentStatsVerySlowly(Action<string> progress, CancellationToken token)
        {
            var sp = Stopwatch.StartNew();
            var stat = new DebugDocumentStats { Total = GetDocumentsCount() };

            var processedDocuments = 0;

            Api.JetSetCurrentIndex(session, Documents, "by_etag");
            Api.MoveBeforeFirst(Session, Documents);
            while (Api.TryMoveNext(Session, Documents))
            {
                if (processedDocuments % 64 == 0)
                {
                    token.ThrowIfCancellationRequested();
                    progress($"Scanned {$"{processedDocuments:#,#;;0}"} documents");
                }

                var key = Api.RetrieveColumnAsString(Session, Documents, tableColumnsCache.DocumentsColumns["key"],
                                                     Encoding.Unicode);

                RavenJObject metadata;
                var metadateSize = GetMetadataFromStorage(out metadata);
                var docSize = Api.RetrieveColumnSize(session, Documents, tableColumnsCache.DocumentsColumns["data"]) ?? -1;
                var totalSize = docSize + metadateSize;
                stat.TotalSize += totalSize;

                if (key.StartsWith("Raven/", StringComparison.OrdinalIgnoreCase))
                {
                    stat.System.Update(totalSize, key);
                }

                var entityName = metadata.Value<string>(Constants.RavenEntityName);
                if (string.IsNullOrEmpty(entityName))
                {
                    stat.NoCollection.Update(totalSize, key);
                }
                else
                {
                    stat.IncrementCollection(entityName, totalSize, key);
                }

                if (metadata.ContainsKey(Constants.RavenDeleteMarker))
                    stat.Tombstones++;

                processedDocuments++;
            }
            stat.TimeToGenerate = sp.Elapsed;
           
            return stat;
        }

        public IEnumerable<JsonDocument> GetDocumentsWithIdStartingWith(string idPrefix, int start, int take, string skipAfter)
        {
            if (take <= 0)
                yield break;
            Api.JetSetCurrentIndex(session, Documents, "by_key");
            Api.MakeKey(session, Documents, skipAfter ?? idPrefix, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Documents, SeekGrbit.SeekGE) == false)
                yield break;

            Api.MakeKey(session, Documents, idPrefix, Encoding.Unicode, MakeKeyGrbit.NewKey | MakeKeyGrbit.SubStrLimit);
            if (
                Api.TrySetIndexRange(session, Documents, SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive) ==
                false)
                yield break;

            if (skipAfter != null && TryMoveTableRecords(Documents, 1, backward: false))
                yield break;

            if (TryMoveTableRecords(Documents, start, backward: false))
                yield break;
            do
            {
                var keyFromDb = Api.RetrieveColumnAsString(session, Documents, tableColumnsCache.DocumentsColumns["key"], Encoding.Unicode);
                if (keyFromDb.StartsWith(idPrefix, StringComparison.OrdinalIgnoreCase) == false)
                {
                    if (StartsWithIgnoreCaseAndSymbols(keyFromDb, idPrefix))
                        continue;
                    yield break;
                }

                yield return ReadCurrentDocument();
                take--;
            } while (Api.TryMoveNext(session, Documents) && take > 0);
        }

        public IEnumerable<JsonDocument> GetDocuments(int start)
        {
            if (start > 0 && TryMoveTableRecords(Documents, start, backward: false))
                yield break;
            do
            {
                yield return ReadCurrentDocument();
            } while (Api.TryMoveNext(session, Documents));
        }

        private bool StartsWithIgnoreCaseAndSymbols(string keyFromDb, string idPrefix)
        {
            var keyPos = 0;
            for (int i = 0; i < idPrefix.Length; i++)
            {
                var idChar = idPrefix[i];
                if(char.IsSymbol(idChar) ||
                    char.IsPunctuation(idChar))
                    continue;
                char keyChar;
                do
                {
                    if (keyPos >= keyFromDb.Length)
                        return false; // too short
                    keyChar = keyFromDb[keyPos++];
                } while (char.IsSymbol(keyChar) || char.IsPunctuation(keyChar));
                if(idChar == keyChar)
                    continue;
                // maybe different casing?
                if(char.ToUpperInvariant(keyChar) ==
                    char.ToUpperInvariant(idChar))
                    continue;
                return false;
            }
            return true;
        }

        public Etag GetEtagAfterSkip(Etag etag, int skip, CancellationToken cancellationToken, out int skipped)
        {
            Api.JetSetCurrentIndex(session, Documents, "by_etag");
            Api.MakeKey(session, Documents, etag.TransformToValueForEsentSorting(), MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Documents, SeekGrbit.SeekGE) == false)
            {
                skipped = 0;
                return etag;
            }

            if (TryMoveTableRecords(Documents, skip, false))
            {
                //skipping failed, will try to move one by one
                Api.JetSetCurrentIndex(session, Documents, "by_etag");
                Api.MakeKey(session, Documents, etag.TransformToValueForEsentSorting(), MakeKeyGrbit.NewKey);
                if (Api.TrySeek(session, Documents, SeekGrbit.SeekGT) == false)
                {
                    skipped = 0;
                    return etag;
                }

                var documentCount = 0;
                bool needPrev;
                do
                {
                    needPrev = false;
                    documentCount++;
                    if (--skip <= 0)
                        break;

                    cancellationToken.ThrowIfCancellationRequested();

                    needPrev = true;
                } while (Api.TryMoveNext(session, Documents));

                skipped = documentCount;
                if (needPrev)
                {
                    if (Api.TryMovePrevious(session, Documents) == false)
                        return etag;
                }
            }
            else
            {
                skipped = skip;
            }

            return Etag.Parse(Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"]));
        }

        public void TouchDocument(string key, out Etag preTouchEtag, out Etag afterTouchEtag)
        {
            Api.JetSetCurrentIndex(session, Documents, "by_key");
            Api.MakeKey(session, Documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
            var isUpdate = Api.TrySeek(session, Documents, SeekGrbit.SeekEQ);
            if (isUpdate == false)
            {
                preTouchEtag = null;
                afterTouchEtag = null;
                return;
            }

            preTouchEtag = Etag.Parse(Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"]));
            Etag newEtag = uuidGenerator.CreateSequentialUuid(UuidType.Documents);
            afterTouchEtag = newEtag;
            try
            {
                if (logger.IsDebugEnabled)
                    logger.Debug("Touching document {0} {1} -> {2}", key, preTouchEtag, afterTouchEtag);
                using (var update = new Update(session, Documents, JET_prep.Replace))
                {
                    Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"], newEtag.TransformToValueForEsentSorting());
                    update.Save();
                }
            }
            catch (EsentErrorException e)
            {
                switch (e.Error)
                {
                    case JET_err.WriteConflict:
                    case JET_err.WriteConflictPrimaryIndex:
                        throw new ConcurrencyException("Cannot touch document " + key + " because it is already modified");
                    default:
                        throw;
                }
            }

            cacher.RemoveCachedDocument(key, preTouchEtag);
            etagTouches.Add(preTouchEtag, afterTouchEtag);
        }

        public void IncrementDocumentCount(int value)
        {
            if (Api.TryMoveFirst(session, Details))
                Api.EscrowUpdate(session, Details, tableColumnsCache.DetailsColumns["document_count"], value);
        }

        public AddDocumentResult AddDocument(string key, Etag etag, RavenJObject data, RavenJObject metadata)
        {
            if (key == null) throw new ArgumentNullException("key");
            var byteCount = Encoding.Unicode.GetByteCount(key);
            if (byteCount >= 2048)
                throw new ArgumentException(string.Format("The key must be a maximum of 2,048 bytes in Unicode, 1,024 characters, key is: '{0}'", key), "key");

            try
            {
                Api.JetSetCurrentIndex(session, Documents, "by_key");
                Api.MakeKey(session, Documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
                var isUpdate = Api.TrySeek(session, Documents, SeekGrbit.SeekEQ);

                Etag existingEtag = null;
                if (isUpdate)
                {
                    existingEtag = EnsureDocumentEtagMatch(key, etag, "PUT");
                }
                else
                {
                    if (etag != null && etag != Etag.Empty) // expected something to be there.
                        throw new ConcurrencyException("PUT attempted on document '" + key +
                                                       "' using a non current etag (document deleted)")
                        {
                            ExpectedETag = etag
                        };
                    if (Api.TryMoveFirst(session, Details))
                        Api.EscrowUpdate(session, Details, tableColumnsCache.DetailsColumns["document_count"], 1);
                }
                Etag newEtag = uuidGenerator.CreateSequentialUuid(UuidType.Documents);

                DateTime savedAt;
                try
                {
                    using (var update = new Update(session, Documents, isUpdate ? JET_prep.Replace : JET_prep.Insert))
                    {
                        Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["key"], key, Encoding.Unicode);
                        using (var columnStream = new ColumnStream(session, Documents, tableColumnsCache.DocumentsColumns["data"]))
                        {
                            if (isUpdate)
                                columnStream.SetLength(0); // empty the existing value, since we are going to overwrite the entire thing
                            using (Stream stream = new BufferedStream(columnStream))
                            using (
                                var finalStream = documentCodecs.Aggregate(stream, (current, codec) => codec.Encode(key, data, metadata, current))
                                )
                            {
                                data.WriteTo(finalStream);
                                finalStream.Flush();
                            }
                        }
                        Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"],
                                      newEtag.TransformToValueForEsentSorting());

                        savedAt = SystemTime.UtcNow;
                        Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["last_modified"], savedAt.ToBinary());

                        using (var columnStream = new ColumnStream(session, Documents, tableColumnsCache.DocumentsColumns["metadata"]))
                        {
                            if (isUpdate)
                                columnStream.SetLength(0);
                            using (Stream stream = new BufferedStream(columnStream))
                            {
                                metadata.WriteTo(stream);
                                stream.Flush();
                            }
                        }


                        update.Save();
                    }
                }
                catch (EsentErrorException e)
                {
                    if (e.Error == JET_err.KeyDuplicate || e.Error == JET_err.WriteConflict)
                        throw new ConcurrencyException("PUT attempted on document '" + key + "' concurrently", e);
                    throw;
                }

                if (logger.IsDebugEnabled)
                    logger.Debug("Inserted a new document with key '{0}', update: {1}, ",
                               key, isUpdate);

                if (existingEtag != null)
                    cacher.RemoveCachedDocument(key, existingEtag);

                return new AddDocumentResult
                {
                    Etag = newEtag,
                    PrevEtag = existingEtag,
                    SavedAt = savedAt,
                    Updated = isUpdate
                };
            }
            catch (EsentKeyDuplicateException e)
            {
                throw new ConcurrencyException("Illegal duplicate key " + key, e);
            }
        }

        public AddDocumentResult InsertDocument(string key, RavenJObject data, RavenJObject metadata, bool overwriteExisting)
        {
            var prep = JET_prep.Insert;
            bool isUpdate = false;

            Etag existingETag = null;
            if (overwriteExisting)
            {
                Api.JetSetCurrentIndex(session, Documents, "by_key");
                Api.MakeKey(session, Documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
                isUpdate = Api.TrySeek(session, Documents, SeekGrbit.SeekEQ);
                if (isUpdate)
                {
                    existingETag = Etag.Parse(Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"]));
                    prep = JET_prep.Replace;
                }
            }

            try
            {
                using (var update = new Update(session, Documents, prep))
                {
                    Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["key"], key, Encoding.Unicode);
                    using (var columnStream = new ColumnStream(session, Documents, tableColumnsCache.DocumentsColumns["data"]))
                    {
                        if (isUpdate)
                            columnStream.SetLength(0);
                        using (Stream stream = new BufferedStream(columnStream))
                        using (var finalStream = documentCodecs.Aggregate(stream, (current, codec) => codec.Encode(key, data, metadata, current)))
                        {
                            data.WriteTo(finalStream);
                            finalStream.Flush();
                        }
                    }
                    Etag newEtag = uuidGenerator.CreateSequentialUuid(UuidType.Documents);
                    Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"], newEtag.TransformToValueForEsentSorting());
                    DateTime savedAt = SystemTime.UtcNow;
                    Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["last_modified"], savedAt.ToBinary());

                    using (var columnStream = new ColumnStream(session, Documents, tableColumnsCache.DocumentsColumns["metadata"]))
                    {
                        if (isUpdate)
                            columnStream.SetLength(0);
                        using (Stream stream = new BufferedStream(columnStream))
                        {
                            metadata.WriteTo(stream);
                            stream.Flush();
                        }
                    }

                    update.Save();

                    if (existingETag != null)
                        cacher.RemoveCachedDocument(key, existingETag);

                    return new AddDocumentResult
                    {
                        Etag = newEtag,
                        PrevEtag = existingETag,
                        SavedAt = savedAt,
                        Updated = isUpdate
                    };
                }
            }
            catch (EsentKeyDuplicateException e)
            {
                throw new ConcurrencyException("Illegal duplicate key " + key, e);
            }
        }

        public bool DeleteDocument(string key, Etag etag, out RavenJObject metadata, out Etag deletedETag)
        {
            metadata = null;
            Api.JetSetCurrentIndex(session, Documents, "by_key");
            Api.MakeKey(session, Documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Documents, SeekGrbit.SeekEQ) == false)
            {
                if (logger.IsDebugEnabled)
                    logger.Debug("Document with key '{0}' was not found, and considered deleted", key);
                deletedETag = null;
                return false;
            }
            if (Api.TryMoveFirst(session, Details))
                Api.EscrowUpdate(session, Details, tableColumnsCache.DetailsColumns["document_count"], -1);

            var existingEtag = EnsureDocumentEtagMatch(key, etag, "DELETE");

            int _;
            metadata = ReadDocumentMetadata(key, existingEtag, out _);
            deletedETag = existingEtag;

            Api.JetDelete(session, Documents);
            if (logger.IsDebugEnabled)
                logger.Debug("Document with key '{0}' was deleted", key);

            cacher.RemoveCachedDocument(key, existingEtag);

            return true;
        }
    }
}
