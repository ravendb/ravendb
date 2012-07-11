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
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Database.Extensions;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Storage.Esent.StorageActions
{
	public partial class DocumentStorageActions : IDocumentStorageActions, ITransactionStorageActions
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
					DataAsJson = createDocument(metadata.Key, metadata.Etag.Value, metadata.Metadata),
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

		private T DocumentByKeyInternal<T>(string key, TransactionInformation transactionInformation, Func<JsonDocumentMetadata, Func<string, Guid, RavenJObject, RavenJObject>, T> createResult)
			where T : class
		{
			bool existsInTx = IsDocumentModifiedInsideTransaction(key);
			
			if (transactionInformation != null && existsInTx)
			{
				var txId = Api.RetrieveColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["locked_by_transaction"]);
				if (new Guid(txId) == transactionInformation.Id)
				{
					if (Api.RetrieveColumnAsBoolean(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["delete_document"]) == true)
					{
						logger.Debug("Document with key '{0}' was deleted in transaction: {1}", key, transactionInformation.Id);
						return null;
					}
					var etag = Api.RetrieveColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["etag"]).TransfromToGuidWithProperSorting();

					RavenJObject metadata = ReadDocumentMetadataInTransaction(key, etag);


					logger.Debug("Document with key '{0}' was found in transaction: {1}", key, transactionInformation.Id);
					return createResult(new JsonDocumentMetadata()
					{
						NonAuthoritativeInformation = false,// we are the transaction, therefor we are Authoritative
						Etag = etag,
						LastModified = Api.RetrieveColumnAsDateTime(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["last_modified"]).Value,
						Key = Api.RetrieveColumnAsString(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["key"], Encoding.Unicode),
						Metadata = metadata
					}, ReadDocumentDataInTransaction);
				}
			}

			Api.JetSetCurrentIndex(session, Documents, "by_key");
			Api.MakeKey(session, Documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Documents, SeekGrbit.SeekEQ) == false)
			{
				if(existsInTx)
				{
					logger.Debug("Committed document with key '{0}' was not found, but exists in a separate transaction", key);
					return createResult(new JsonDocumentMetadata
					{
						Etag = Guid.Empty,
						Key = key,
						Metadata = new RavenJObject{{Constants.RavenDocumentDoesNotExists, true}},
						NonAuthoritativeInformation = true,
						LastModified = DateTime.MinValue,
					}, (docKey, etag, metadata) => new RavenJObject());
				}
				logger.Debug("Document with key '{0}' was not found", key);
				return null;
			}
			var existingEtag = Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"]).TransfromToGuidWithProperSorting();
			logger.Debug("Document with key '{0}' was found", key);
			return createResult(new JsonDocumentMetadata()
			{
				Etag = existingEtag,
				NonAuthoritativeInformation = existsInTx,
				LastModified = Api.RetrieveColumnAsDateTime(session, Documents, tableColumnsCache.DocumentsColumns["last_modified"]).Value,
				Key = Api.RetrieveColumnAsString(session, Documents, tableColumnsCache.DocumentsColumns["key"], Encoding.Unicode),
				Metadata = ReadDocumentMetadata(key, existingEtag)
			}, ReadDocumentData);
		}

		private RavenJObject ReadDocumentMetadataInTransaction(string key, Guid etag)
		{
			var cachedDocument = cacher.GetCachedDocument(key, etag);
			if (cachedDocument != null)
			{
				return cachedDocument.Metadata;
			}

			return Api.RetrieveColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["metadata"]).ToJObject();

		}

		private RavenJObject ReadDocumentDataInTransaction(string key, Guid etag, RavenJObject metadata)
		{
			var cachedDocument = cacher.GetCachedDocument(key, etag);
			if (cachedDocument != null)
			{
				return cachedDocument.Document;
			}

			using (Stream stream = new BufferedStream(new ColumnStream(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["data"])))
			{
				var size = stream.Length;
				using(var aggregate = documentCodecs.Aggregate(stream, (bytes, codec) => codec.Decode(key, metadata, bytes)))
				{
					var data = aggregate.ToJObject();
					cacher.SetCachedDocument(key, etag, data, metadata, (int) size);
					return data;
				}
			}
		}

		private RavenJObject ReadDocumentMetadata(string key, Guid existingEtag)
		{
			var existingCachedDocument = cacher.GetCachedDocument(key, existingEtag);
			if (existingCachedDocument != null)
				return existingCachedDocument.Metadata;

			return Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["metadata"]).ToJObject();
		}

		private RavenJObject ReadDocumentData(string key, Guid existingEtag, RavenJObject metadata)
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
			for (int i = 0; i < start; i++)
			{
				if (Api.TryMovePrevious(session, Documents) == false)
					return Enumerable.Empty<JsonDocument>();
			}
			var optimizer = new OptimizedIndexReader(Session, Documents, take);
			while (Api.TryMovePrevious(session, Documents) && optimizer.Count < take)
			{
				optimizer.Add();
			}

			return optimizer.Select(ReadCurrentDocument);
		}

		private JsonDocument ReadCurrentDocument()
		{
			var metadataSize = Api.RetrieveColumnSize(session, Documents,tableColumnsCache.DocumentsColumns["metadata"]) ?? 0;
			var docSize = Api.RetrieveColumnSize(session, Documents, tableColumnsCache.DocumentsColumns["data"]) ?? 0;

			var metadata = Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["metadata"]).ToJObject();
			var key = Api.RetrieveColumnAsString(session, Documents, tableColumnsCache.DocumentsColumns["key"], Encoding.Unicode);

			RavenJObject dataAsJson;
			using (
				Stream stream = new BufferedStream(new ColumnStream(session, Documents, tableColumnsCache.DocumentsColumns["data"])))
			{
				using (var aggregate = documentCodecs.Aggregate(stream, (bytes, codec) => codec.Decode(key, metadata, bytes)))
					dataAsJson = aggregate.ToJObject();
			}

			return new JsonDocument
			{
				SerializedSizeOnDisk = metadataSize + docSize,
				Key = key,
				DataAsJson = dataAsJson,
				NonAuthoritativeInformation = IsDocumentModifiedInsideTransaction(key),
				LastModified =
					Api.RetrieveColumnAsDateTime(session, Documents, tableColumnsCache.DocumentsColumns["last_modified"]).Value,
				Etag =
					Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"]).TransfromToGuidWithProperSorting(),
				Metadata = metadata
			};
		}


		public IEnumerable<JsonDocument> GetDocumentsAfter(Guid etag, int take, long? maxSize = null)
		{
			Api.JetSetCurrentIndex(session, Documents, "by_etag");
			Api.MakeKey(session, Documents, etag.TransformToValueForEsentSorting(), MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Documents, SeekGrbit.SeekGT) == false)
				yield break;
			long totalSize = 0;
			int count = 0;
			do
			{
				var readCurrentDocument = ReadCurrentDocument();
				totalSize += readCurrentDocument.SerializedSizeOnDisk;
				if(maxSize != null && totalSize > maxSize.Value)
				{
					yield return readCurrentDocument;
					yield break;
				}
				yield return readCurrentDocument;
				count++;
			} while (Api.TryMoveNext(session, Documents) && count < take);
		}


		public IEnumerable<JsonDocument> GetDocumentsWithIdStartingWith(string idPrefix, int start, int take)
		{
			Api.JetSetCurrentIndex(session, Documents, "by_key");
			Api.MakeKey(session, Documents, idPrefix, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Documents, SeekGrbit.SeekGE) == false)
				return Enumerable.Empty<JsonDocument>();

			var optimizer = new OptimizedIndexReader(Session, Documents, take);
			do
			{
				Api.MakeKey(session, Documents, idPrefix, Encoding.Unicode, MakeKeyGrbit.NewKey | MakeKeyGrbit.SubStrLimit);
				if (Api.TrySetIndexRange(session, Documents, SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive) == false)
					return Enumerable.Empty<JsonDocument>();

				while (start > 0)
				{
					if (Api.TryMoveNext(session, Documents) == false)
						return Enumerable.Empty<JsonDocument>();
					start--;
				}

				optimizer.Add();

			} while (Api.TryMoveNext(session, Documents) && optimizer.Count < take);

			return optimizer.Select(ReadCurrentDocument);
		}

		public Guid AddDocument(string key, Guid? etag, RavenJObject data, RavenJObject metadata)
		{
			if (key != null && Encoding.Unicode.GetByteCount(key) >= 2048)
				throw new ArgumentException(string.Format("The key must be a maximum of 2,048 bytes in Unicode, 1,024 characters, key is: '{0}'", key), "key");

			Api.JetSetCurrentIndex(session, Documents, "by_key");
			Api.MakeKey(session, Documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			var isUpdate = Api.TrySeek(session, Documents, SeekGrbit.SeekEQ);
			if (isUpdate)
			{
				EnsureNotLockedByTransaction(key, null);
				EnsureDocumentEtagMatch(key, etag, "PUT");

			}
			else
			{
				if (etag != null && etag != Guid.Empty) // expected something to be there.
					throw new ConcurrencyException("PUT attempted on document '" + key +
					                               "' using a non current etag (document deleted)")
					{
						ExpectedETag = etag.Value
					};
				EnsureDocumentIsNotCreatedInAnotherTransaction(key, Guid.NewGuid());
				if (Api.TryMoveFirst(session, Details))
					Api.EscrowUpdate(session, Details, tableColumnsCache.DetailsColumns["document_count"], 1);
			}
			Guid newEtag = uuidGenerator.CreateSequentialUuid();


			using (var update = new Update(session, Documents, isUpdate ? JET_prep.Replace : JET_prep.Insert))
			{
				Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["key"], key, Encoding.Unicode);
				using (Stream stream = new BufferedStream(new ColumnStream(session, Documents, tableColumnsCache.DocumentsColumns["data"])))
				using(var finalStream = documentCodecs.Aggregate(stream, (current, codec) => codec.Encode(key, data, metadata, current)))
				{
					data.WriteTo(finalStream);
					stream.Flush();
				}

				Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"], newEtag.TransformToValueForEsentSorting());
				Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["last_modified"], SystemTime.UtcNow);

				using (Stream stream = new BufferedStream(new ColumnStream(session, Documents, tableColumnsCache.DocumentsColumns["metadata"])))
				{
					metadata.WriteTo(stream);
					stream.Flush();
				}

				update.Save();
			}

			logger.Debug("Inserted a new document with key '{0}', update: {1}, ",
							   key, isUpdate);

			cacher.RemoveCachedDocument(key, newEtag);
			return newEtag;
		}


		public Guid AddDocumentInTransaction(string key, Guid? etag, RavenJObject data, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			Api.JetSetCurrentIndex(session, Documents, "by_key");
			Api.MakeKey(session, Documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			var isUpdate = Api.TrySeek(session, Documents, SeekGrbit.SeekEQ);
			if (isUpdate)
			{
				EnsureNotLockedByTransaction(key, transactionInformation.Id);
				EnsureDocumentEtagMatchInTransaction(key, etag);
				using (var update = new Update(session, Documents, JET_prep.Replace))
				{
					Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["locked_by_transaction"], transactionInformation.Id.ToByteArray());
					update.Save();
				}
			}
			else
			{
				EnsureDocumentIsNotCreatedInAnotherTransaction(key, transactionInformation.Id);
			}
			EnsureTransactionExists(transactionInformation);
			Guid newEtag = uuidGenerator.CreateSequentialUuid();

			Api.JetSetCurrentIndex(session, DocumentsModifiedByTransactions, "by_key");
			Api.MakeKey(session, DocumentsModifiedByTransactions, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			var isUpdateInTransaction = Api.TrySeek(session, DocumentsModifiedByTransactions, SeekGrbit.SeekEQ);

			using (var update = new Update(session, DocumentsModifiedByTransactions, isUpdateInTransaction ? JET_prep.Replace : JET_prep.Insert))
			{
				Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["key"], key, Encoding.Unicode);

				using (Stream stream = new BufferedStream(new ColumnStream(session, DocumentsModifiedByTransactions,tableColumnsCache.DocumentsModifiedByTransactionsColumns["data"])))
				using (var finalStream = documentCodecs.Aggregate(stream, (current, codec) => codec.Encode(key, data, metadata, current)))
				{
					data.WriteTo(finalStream);
					finalStream.Flush();
				}
				Api.SetColumn(session, DocumentsModifiedByTransactions,
							  tableColumnsCache.DocumentsModifiedByTransactionsColumns["etag"],
							  newEtag.TransformToValueForEsentSorting());

				using (Stream stream = new BufferedStream(new ColumnStream(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["metadata"])))
				{
					metadata.WriteTo(stream);
					stream.Flush();
				}

				Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["last_modified"], SystemTime.UtcNow);
				Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["delete_document"], false);
				Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["locked_by_transaction"], transactionInformation.Id.ToByteArray());

				update.Save();
			}
			logger.Debug("Inserted a new document with key '{0}', update: {1}, in transaction: {2}",
							   key, isUpdate, transactionInformation.Id);

			return newEtag;
		}


		public bool DeleteDocument(string key, Guid? etag, out RavenJObject metadata)
		{
			metadata = null;
			Api.JetSetCurrentIndex(session, Documents, "by_key");
			Api.MakeKey(session, Documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Documents, SeekGrbit.SeekEQ) == false)
			{
				logger.Debug("Document with key '{0}' was not found, and considered deleted", key);
				return false;
			}
			if (Api.TryMoveFirst(session, Details))
				Api.EscrowUpdate(session, Details, tableColumnsCache.DetailsColumns["document_count"], -1);

			var existingEtag = EnsureDocumentEtagMatch(key, etag, "DELETE");
			EnsureNotLockedByTransaction(key, null);

			metadata = Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["metadata"]).ToJObject();

			Api.JetDelete(session, Documents);
			logger.Debug("Document with key '{0}' was deleted", key);

			cacher.RemoveCachedDocument(key, existingEtag);

			return true;
		}


		public bool DeleteDocumentInTransaction(TransactionInformation transactionInformation, string key, Guid? etag)
		{
			Api.JetSetCurrentIndex(session, Documents, "by_key");
			Api.MakeKey(session, Documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Documents, SeekGrbit.SeekEQ) == false)
			{
				logger.Debug("Document with key '{0}' was not found, and considered deleted", key);
				return false;
			}

			EnsureNotLockedByTransaction(key, transactionInformation.Id);
			EnsureDocumentEtagMatchInTransaction(key, etag);

			using (var update = new Update(session, Documents, JET_prep.Replace))
			{
				Api.SetColumn(session, Documents, tableColumnsCache.DocumentsColumns["locked_by_transaction"], transactionInformation.Id.ToByteArray());
				update.Save();
			}
			EnsureTransactionExists(transactionInformation);

			Guid newEtag = uuidGenerator.CreateSequentialUuid();

			Api.JetSetCurrentIndex(session, DocumentsModifiedByTransactions, "by_key");
			Api.MakeKey(session, DocumentsModifiedByTransactions, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			var isUpdateInTransaction = Api.TrySeek(session, DocumentsModifiedByTransactions, SeekGrbit.SeekEQ);

			using (var update = new Update(session, DocumentsModifiedByTransactions, isUpdateInTransaction ? JET_prep.Replace : JET_prep.Insert))
			{
				Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["key"], key, Encoding.Unicode);
				Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["data"],
					Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["data"]));
				Api.SetColumn(session, DocumentsModifiedByTransactions,
							  tableColumnsCache.DocumentsModifiedByTransactionsColumns["etag"],
							  newEtag.TransformToValueForEsentSorting());
				Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["last_modified"],
					Api.RetrieveColumnAsDateTime(session, Documents, tableColumnsCache.DocumentsColumns["last_modified"]).Value);
				Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["metadata"],
					Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["metadata"]));
				Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["delete_document"], true);
				Api.SetColumn(session, DocumentsModifiedByTransactions, tableColumnsCache.DocumentsModifiedByTransactionsColumns["locked_by_transaction"], transactionInformation.Id.ToByteArray());

				update.Save();
			}

			return true;
		}


		public Tuple<int, int> FirstAndLastDocumentIds()
		{
			var item1 = 0;
			var item2 = 0;
			Api.JetSetCurrentIndex(session, Documents, "by_id");
			Api.MoveBeforeFirst(session, Documents);
			if (Api.TryMoveNext(session, Documents))
				item1 = Api.RetrieveColumnAsInt32(session, Documents, tableColumnsCache.DocumentsColumns["id"]).Value;
			Api.MoveAfterLast(session, Documents);
			if (Api.TryMovePrevious(session, Documents))
				item2 = Api.RetrieveColumnAsInt32(session, Documents, tableColumnsCache.DocumentsColumns["id"]).Value;
			return new Tuple<int, int>(item1, item2);
		}
	}
}
