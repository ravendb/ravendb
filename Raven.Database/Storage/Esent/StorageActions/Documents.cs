//-----------------------------------------------------------------------
// <copyright file="Documents.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Impl;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Storage.Esent.StorageActions
{
	public partial class DocumentStorageActions : IDocumentStorageActions
	{
		public long GetDocumentsCount()
		{
			if (Api.TryMoveFirst(session, Details))
				return Api.RetrieveColumnAsInt32(session, Details, tableColumnsCache.DetailsColumns["document_count"]).Value;
			return 0;
		}

		public JsonDocument DocumentByKey(string key, TransactionInformation transactionInformation)
		{
			return DocumentByKeyInternal(key, transactionInformation, (metadata, createDocument) =>
			{
				Debug.Assert(metadata.Etag != null);
				return new JsonDocument
				{
					DataAsJson = createDocument(metadata.Key, metadata.Etag, metadata.Metadata),
					Etag = metadata.Etag,
					Key = metadata.Key,
					LastModified = metadata.LastModified,
					Metadata = metadata.Metadata,
					NonAuthoritativeInformation = metadata.NonAuthoritativeInformation,
				};
			});
		}

		public JsonDocumentMetadata DocumentMetadataByKey(string key, TransactionInformation transactionInformation)
		{
			return DocumentByKeyInternal(key, transactionInformation, (metadata, func) => metadata);
		}

		private T DocumentByKeyInternal<T>(string key, TransactionInformation transactionInformation, Func<JsonDocumentMetadata, Func<string, Etag, RavenJObject, RavenJObject>, T> createResult)
			where T : class
		{

			Api.JetSetCurrentIndex(session, Documents, "by_key");
			Api.MakeKey(session, Documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Documents, SeekGrbit.SeekEQ) == false)
			{
				logger.Debug("Document with key '{0}' was not found", key);
				return null;
			}
			var existingEtag = Etag.Parse(Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"]));
			logger.Debug("Document with key '{0}' was found", key);
			var lastModifiedInt64 = Api.RetrieveColumnAsInt64(session, Documents, tableColumnsCache.DocumentsColumns["last_modified"]).Value;
			return createResult(new JsonDocumentMetadata()
			{
				Etag = existingEtag,
				LastModified = DateTime.FromBinary(lastModifiedInt64),
				Key = Api.RetrieveColumnAsString(session, Documents, tableColumnsCache.DocumentsColumns["key"], Encoding.Unicode),
				Metadata = ReadDocumentMetadata(key, existingEtag)
			}, ReadDocumentData);
		}

		private RavenJObject ReadDocumentMetadata(string key, Etag existingEtag)
		{
			var existingCachedDocument = cacher.GetCachedDocument(key, existingEtag);
			if (existingCachedDocument != null)
				return existingCachedDocument.Metadata;

			return Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["metadata"]).ToJObject();
		}

		private RavenJObject ReadDocumentData(string key, Etag existingEtag, RavenJObject metadata)
		{
			var existingCachedDocument = cacher.GetCachedDocument(key, existingEtag);
			if (existingCachedDocument != null)
				return existingCachedDocument.Document;


			using (Stream stream = new BufferedStream(new ColumnStream(session, Documents, tableColumnsCache.DocumentsColumns["data"])))
			{
				var size = stream.Length;
				using (var columnStream = documentCodecs.Aggregate(stream, (dataStream, codec) => codec.Decode(key, metadata, dataStream)))
				{
					var data = columnStream.ToJObject();

					cacher.SetCachedDocument(key, existingEtag, data, metadata, (int)size);

					return data;
				}
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

		private JsonDocument ReadCurrentDocument()
		{
			int docSize;

			var metadataBuffer = Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["metadata"]);
			var metadata = metadataBuffer.ToJObject();
			var key = Api.RetrieveColumnAsString(session, Documents, tableColumnsCache.DocumentsColumns["key"], Encoding.Unicode);

			RavenJObject dataAsJson;
			using (Stream stream = new BufferedStream(new ColumnStream(session, Documents, tableColumnsCache.DocumentsColumns["data"])))
			{
				using (var aggregate = documentCodecs.Aggregate(stream, (bytes, codec) => codec.Decode(key, metadata, bytes)))
				{
					dataAsJson = aggregate.ToJObject();
					docSize = (int)stream.Position;
				}
			}

			bool isDocumentModifiedInsideTransaction = false;
			var lastModified = Api.RetrieveColumnAsInt64(session, Documents, tableColumnsCache.DocumentsColumns["last_modified"]).Value;
			return new JsonDocument
			{
				SerializedSizeOnDisk = metadataBuffer.Length + docSize,
				Key = key,
				DataAsJson = dataAsJson,
				NonAuthoritativeInformation = isDocumentModifiedInsideTransaction,
				LastModified = DateTime.FromBinary(lastModified),
				Etag = Etag.Parse(Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"])),
				Metadata = metadata
			};
		}


		public IEnumerable<JsonDocument> GetDocumentsAfter(Etag etag, int take, long? maxSize = null, Etag untilEtag = null)
		{
			Api.JetSetCurrentIndex(session, Documents, "by_etag");
			Api.MakeKey(session, Documents, etag.TransformToValueForEsentSorting(), MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Documents, SeekGrbit.SeekGT) == false)
				yield break;
			long totalSize = 0;
			int count = 0;
			do
			{
				if (untilEtag != null && count > 0)
				{
					var docEtag = Etag.Parse(Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"]));
					if (EtagUtil.IsGreaterThan(docEtag, untilEtag))
						yield break;
				}
				var readCurrentDocument = ReadCurrentDocument();
				totalSize += readCurrentDocument.SerializedSizeOnDisk;
				if (maxSize != null && totalSize > maxSize.Value)
				{
					yield return readCurrentDocument;
					yield break;
				}
				yield return readCurrentDocument;
				count++;
			} while (Api.TryMoveNext(session, Documents) && count < take);
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

	    public DebugDocumentStats GetDocumentStatsVerySlowly()
	    {
	        var sp = Stopwatch.StartNew();
            var stat = new DebugDocumentStats { Total = GetDocumentsCount() };

            Api.JetSetCurrentIndex(session, Documents, "by_etag");
			Api.MoveBeforeFirst(Session, Documents);
	        while (Api.TryMoveNext(Session, Documents))
	        {
	            var key = Api.RetrieveColumnAsString(Session, Documents, tableColumnsCache.DocumentsColumns["key"],
	                                                 Encoding.Unicode);
	            if (key.StartsWith("Raven/", StringComparison.OrdinalIgnoreCase))
	                stat.System++;

	            var metadata =
	                Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["metadata"]).ToJObject();

	            var entityName = metadata.Value<string>(Constants.RavenEntityName);
	            if (string.IsNullOrEmpty(entityName))
	                stat.NoCollection++;
	            else
	                stat.IncrementCollection(entityName);

	            if (metadata.ContainsKey("Raven-Delete-Marker"))
	                stat.Tombstones++;
	        }

	        stat.TimeToGenerate = sp.Elapsed;
	        return stat;
	    }

	    public IEnumerable<JsonDocument> GetDocumentsWithIdStartingWith(string idPrefix, int start, int take)
		{
			if (take <= 0)
				yield break;
			Api.JetSetCurrentIndex(session, Documents, "by_key");
			Api.MakeKey(session, Documents, idPrefix, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Documents, SeekGrbit.SeekGE) == false)
				yield break;

			Api.MakeKey(session, Documents, idPrefix, Encoding.Unicode, MakeKeyGrbit.NewKey | MakeKeyGrbit.SubStrLimit);
			if (
				Api.TrySetIndexRange(session, Documents, SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive) ==
				false)
				yield break;

			if (TryMoveTableRecords(Documents, start, backward: false))
				yield break;
			do
			{
				yield return ReadCurrentDocument();
				take--;
			} while (Api.TryMoveNext(session, Documents) && take > 0);
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


				logger.Debug("Inserted a new document with key '{0}', update: {1}, ",
							   key, isUpdate);

				cacher.RemoveCachedDocument(key, newEtag);
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
				logger.Debug("Document with key '{0}' was not found, and considered deleted", key);
				deletedETag = null;
				return false;
			}
			if (Api.TryMoveFirst(session, Details))
				Api.EscrowUpdate(session, Details, tableColumnsCache.DetailsColumns["document_count"], -1);

			var existingEtag = EnsureDocumentEtagMatch(key, etag, "DELETE");

			metadata = Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["metadata"]).ToJObject();
			deletedETag = existingEtag;

			Api.JetDelete(session, Documents);
			logger.Debug("Document with key '{0}' was deleted", key);

			cacher.RemoveCachedDocument(key, existingEtag);

			return true;
		}
	}
}
