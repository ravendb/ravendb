namespace Raven.Database.Storage.Voron.StorageActions
{
	using System.Linq;

	using Raven.Abstractions.Logging;
	using Raven.Abstractions.Util;

	using System;
	using System.Collections.Generic;
	using System.IO;
	using System.Text;

	using Raven.Abstractions;
	using Raven.Abstractions.Data;
	using Raven.Abstractions.Exceptions;
	using Raven.Abstractions.MEF;
    using Raven.Abstractions.Util.Streams;
	using Raven.Database.Impl;
	using Raven.Database.Plugins;
	using Raven.Database.Storage.Voron.Impl;
	using Raven.Json.Linq;
	using Raven.Abstractions.Extensions;

	using global::Voron;
	using global::Voron.Impl;

	using Constants = Raven.Abstractions.Data.Constants;

	public class DocumentsStorageActions : StorageActionsBase, IDocumentStorageActions
	{
		private readonly Reference<WriteBatch> writeBatch;

		private readonly IUuidGenerator uuidGenerator;
		private readonly OrderedPartCollection<AbstractDocumentCodec> documentCodecs;
		private readonly IDocumentCacher documentCacher;

		private static readonly ILog logger = LogManager.GetCurrentClassLogger();
        private readonly Dictionary<Etag, Etag> etagTouches = new Dictionary<Etag, Etag>();
		private readonly TableStorage tableStorage;

		private readonly Index metadataIndex;

		public DocumentsStorageActions(IUuidGenerator uuidGenerator,
			OrderedPartCollection<AbstractDocumentCodec> documentCodecs,
			IDocumentCacher documentCacher,
			Reference<WriteBatch> writeBatch,
			Reference<SnapshotReader> snapshot,
            TableStorage tableStorage, 
            IBufferPool bufferPool)
			: base(snapshot, bufferPool)
		{
			this.uuidGenerator = uuidGenerator;
			this.documentCodecs = documentCodecs;
			this.documentCacher = documentCacher;
			this.writeBatch = writeBatch;
			this.tableStorage = tableStorage;

			metadataIndex = tableStorage.Documents.GetIndex(Tables.Documents.Indices.Metadata);
		}

		public IEnumerable<JsonDocument> GetDocumentsByReverseUpdateOrder(int start, int take)
		{
			if (start < 0)
				throw new ArgumentException("must have zero or positive value", "start");
			if (take < 0)
				throw new ArgumentException("must have zero or positive value", "take");
			if (take == 0) yield break;

			using (var iterator = tableStorage.Documents.GetIndex(Tables.Documents.Indices.KeyByEtag)
											.Iterate(Snapshot, writeBatch.Value))
			{
				int fetchedDocumentCount = 0;
				if (!iterator.Seek(Slice.AfterAllKeys))
					yield break;

				if (!iterator.Skip(-start))
					yield break;
				do
				{
					if (iterator.CurrentKey == null || iterator.CurrentKey.Equals(Slice.Empty))
						yield break;

					var key = GetKeyFromCurrent(iterator);

					var document = DocumentByKey(key, null);
					if (document == null) //precaution - should never be true
					{
						throw new ApplicationException(string.Format("Possible data corruption - the key = '{0}' was found in the documents indice, but matching document was not found.", key));
					}

					yield return document;

					fetchedDocumentCount++;
				} while (iterator.MovePrev() && fetchedDocumentCount < take);
			}
		}

		public IEnumerable<JsonDocument> GetDocumentsAfter(Etag etag, int take, long? maxSize = null, Etag untilEtag = null)
		{
			if (take < 0)
				throw new ArgumentException("must have zero or positive value", "take");
			if (take == 0) yield break;

			if (string.IsNullOrEmpty(etag))
				throw new ArgumentNullException("etag");

			using (var iterator = tableStorage.Documents.GetIndex(Tables.Documents.Indices.KeyByEtag)
											.Iterate(Snapshot, writeBatch.Value))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
				{
					yield break;
				}

				long fetchedDocumentTotalSize = 0;
				int fetchedDocumentCount = 0;

				do
				{
					if (iterator.CurrentKey == null || iterator.CurrentKey.Equals(Slice.Empty))
						yield break;

					var docEtag = Etag.Parse(iterator.CurrentKey.ToString());

					if (!EtagUtil.IsGreaterThan(docEtag, etag)) continue;

					if (untilEtag != null && fetchedDocumentCount > 0)
					{
						if (EtagUtil.IsGreaterThan(docEtag, untilEtag))
							yield break;
					}

					var key = GetKeyFromCurrent(iterator);

					var document = DocumentByKey(key, null);
					if (document == null) //precaution - should never be true
					{
						throw new ApplicationException(string.Format("Possible data corruption - the key = '{0}' was found in the documents indice, but matching document was not found", key));
					}

					fetchedDocumentTotalSize += document.SerializedSizeOnDisk;
					fetchedDocumentCount++;

					if (maxSize.HasValue && fetchedDocumentTotalSize >= maxSize)
					{
						yield return document;
						yield break;
					}

					yield return document;
				} while (iterator.MoveNext() && fetchedDocumentCount < take);
			}
		}

		private static string GetKeyFromCurrent(global::Voron.Trees.IIterator iterator)
		{
			string key;
			using (var currentDataStream = iterator.CreateReaderForCurrent().AsStream())
			{
				var keyBytes = currentDataStream.ReadData();
				key = Encoding.UTF8.GetString(keyBytes);
			}
			return key;
		}

		public IEnumerable<JsonDocument> GetDocumentsWithIdStartingWith(string idPrefix, int start, int take)
		{
			if (string.IsNullOrEmpty(idPrefix))
				throw new ArgumentNullException("idPrefix");
			if (start < 0)
				throw new ArgumentException("must have zero or positive value", "start");
			if (take < 0)
				throw new ArgumentException("must have zero or positive value", "take");

			if (take == 0)
				yield break;

			using (var iterator = tableStorage.Documents.Iterate(Snapshot, writeBatch.Value))
			{
				iterator.RequiredPrefix = idPrefix.ToLowerInvariant();
				if (iterator.Seek(iterator.RequiredPrefix) == false || !iterator.Skip(start))
					yield break;

				var fetchedDocumentCount = 0;
				do
				{
					var key = iterator.CurrentKey.ToString();

					var fetchedDocument = DocumentByKey(key, null);
					if (fetchedDocument == null) continue;

					fetchedDocumentCount++;
					yield return fetchedDocument;
				} while (iterator.MoveNext() && fetchedDocumentCount < take);
			}
		}

		public long GetDocumentsCount()
		{
			return tableStorage.GetEntriesCount(tableStorage.Documents);
		}

		public JsonDocument DocumentByKey(string key, TransactionInformation transactionInformation)
		{
			if (string.IsNullOrEmpty(key))
			{
				logger.Debug("Document with empty key was not found");
				return null;
			}

			var lowerKey = CreateKey(key);
			if (!tableStorage.Documents.Contains(Snapshot, lowerKey, writeBatch.Value))
			{
				logger.Debug("Document with key='{0}' was not found", key);
				return null;
			}

			var metadataDocument = ReadDocumentMetadata(key);
			if (metadataDocument == null)
			{
				logger.Warn(string.Format("Metadata of document with key='{0} was not found, but the document itself exists.", key));
				return null;
			}

			var documentData = ReadDocumentData(key, metadataDocument.Etag, metadataDocument.Metadata);

			logger.Debug("DocumentByKey() by key ='{0}'", key);

			var docSize = tableStorage.Documents.GetDataSize(Snapshot, lowerKey);

			var metadataSize = metadataIndex.GetDataSize(Snapshot, lowerKey);

			return new JsonDocument
			{
				DataAsJson = documentData,
				Etag = metadataDocument.Etag,
				Key = metadataDocument.Key, //original key - with user specified casing, etc.
				Metadata = metadataDocument.Metadata,
				SerializedSizeOnDisk = docSize + metadataSize,
				LastModified = metadataDocument.LastModified
			};
		}

		public JsonDocumentMetadata DocumentMetadataByKey(string key, TransactionInformation transactionInformation)
		{
			if (string.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");

			var lowerKey = CreateKey(key);

			if (tableStorage.Documents.Contains(Snapshot, lowerKey, writeBatch.Value))
				return ReadDocumentMetadata(key);

			logger.Debug("Document with key='{0}' was not found", key);
			return null;
		}

		public bool DeleteDocument(string key, Etag etag, out RavenJObject metadata, out Etag deletedETag)
		{
			if (string.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");

			var loweredKey = CreateKey(key);
			
			if(etag != null)
				EnsureDocumentEtagMatch(loweredKey, etag, "DELETE");

			ushort? existingVersion;
			if (!tableStorage.Documents.Contains(Snapshot, loweredKey, writeBatch.Value, out existingVersion))
			{
				logger.Debug("Document with key '{0}' was not found, and considered deleted", key);
				metadata = null;
				deletedETag = null;
				return false;
			}

			if (!metadataIndex.Contains(Snapshot, loweredKey, writeBatch.Value)) //data exists, but metadata is not --> precaution, should never be true
			{
				var errorString = string.Format("Document with key '{0}' was found, but its metadata wasn't found --> possible data corruption", key);
				throw new ApplicationException(errorString);
			}

			var existingEtag = EnsureDocumentEtagMatch(key, etag, "DELETE");
			var documentMetadata = ReadDocumentMetadata(key);
			metadata = documentMetadata.Metadata;

			deletedETag = etag != null ? existingEtag : documentMetadata.Etag;

			tableStorage.Documents.Delete(writeBatch.Value, loweredKey, existingVersion);
			metadataIndex.Delete(writeBatch.Value, loweredKey);

			tableStorage.Documents.GetIndex(Tables.Documents.Indices.KeyByEtag)
						  .Delete(writeBatch.Value, deletedETag);

			documentCacher.RemoveCachedDocument(loweredKey, etag);

			logger.Debug("Deleted document with key = '{0}'", key);

			return true;
		}

		public AddDocumentResult AddDocument(string key, Etag etag, RavenJObject data, RavenJObject metadata)
		{
			if (string.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");

			if (key != null && Encoding.UTF8.GetByteCount(key) >= UInt16.MaxValue)
				throw new ArgumentException(string.Format("The dataKey must be a maximum of {0} bytes in Unicode, key is: '{1}'", UInt16.MaxValue, key), "key");

			Etag existingEtag;
			Etag newEtag;

			DateTime savedAt;
			var isUpdate = WriteDocumentData(key, etag, data, metadata, out newEtag, out existingEtag, out savedAt);

			logger.Debug("AddDocument() - {0} document with key = '{1}'", isUpdate ? "Updated" : "Added", key);

			return new AddDocumentResult
			{
				Etag = newEtag,
				PrevEtag = existingEtag,
				SavedAt = savedAt,
				Updated = isUpdate
			};
		}

		private bool PutDocumentMetadataInternal(string key, RavenJObject metadata, Etag newEtag, DateTime savedAt)
		{
			return WriteDocumentMetadata(new JsonDocumentMetadata
			{
				Key = key,
				Etag = newEtag,
				Metadata = metadata,
				LastModified = savedAt
			});
		}

		public void IncrementDocumentCount(int value)
		{
			//nothing to do here			
		}

		public AddDocumentResult InsertDocument(string key, RavenJObject data, RavenJObject metadata, bool overwriteExisting)
		{
			if (string.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");

			if (!overwriteExisting && tableStorage.Documents.Contains(Snapshot, CreateKey(key), writeBatch.Value))
			{
				throw new ConcurrencyException(string.Format("InsertDocument() - overwriteExisting is false and document with key = '{0}' already exists", key));
			}

			return AddDocument(key, null, data, metadata);
		}

		public void TouchDocument(string key, out Etag preTouchEtag, out Etag afterTouchEtag)
		{
			if (string.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");

			var lowerKey = CreateKey(key);

			if (!tableStorage.Documents.Contains(Snapshot, lowerKey, writeBatch.Value))
			{
				logger.Debug("Document with dataKey='{0}' was not found", key);
				preTouchEtag = null;
				afterTouchEtag = null;
				return;
			}

			var metadata = ReadDocumentMetadata(key);

			var newEtag = uuidGenerator.CreateSequentialUuid(UuidType.Documents);
			afterTouchEtag = newEtag;
			preTouchEtag = metadata.Etag;
			metadata.Etag = newEtag;

			WriteDocumentMetadata(metadata,shouldIgnoreConcurrencyExceptions:true);

			var keyByEtagIndex = tableStorage.Documents.GetIndex(Tables.Documents.Indices.KeyByEtag);

			keyByEtagIndex.Delete(writeBatch.Value, preTouchEtag);
			keyByEtagIndex.Add(writeBatch.Value, newEtag, lowerKey);
            etagTouches.Add(preTouchEtag, afterTouchEtag);

			logger.Debug("TouchDocument() - document with key = '{0}'", key);
		}

		public Etag GetBestNextDocumentEtag(Etag etag)
		{
			if (etag == null) throw new ArgumentNullException("etag");

			using (var iter = tableStorage.Documents.GetIndex(Tables.Documents.Indices.KeyByEtag)
											.Iterate(Snapshot, writeBatch.Value))
			{
				if (!iter.Seek(etag.ToString()) &&
					!iter.Seek(Slice.BeforeAllKeys)) //if parameter etag not found, scan from beginning. if empty --> return original etag
					return etag;

				do
				{
					var docEtag = Etag.Parse(iter.CurrentKey.ToString());
					if (EtagUtil.IsGreaterThan(docEtag, etag))
						return docEtag;
				} while (iter.MoveNext());
			}

			return etag; //if not found, return the original etag
		}

		private Etag EnsureDocumentEtagMatch(string key, Etag etag, string method)
		{
			var metadata = ReadDocumentMetadata(key);

			if (metadata == null)
				return Etag.InvalidEtag;

			var existingEtag = metadata.Etag;

           
			if (etag != null)
			{
                Etag next;
                while (etagTouches.TryGetValue(etag, out next))
                {
                    etag = next;
                }

				if (existingEtag != etag)
				{
					if (etag == Etag.Empty)
					{
						if (metadata.Metadata.ContainsKey(Constants.RavenDeleteMarker) &&
							metadata.Metadata.Value<bool>(Constants.RavenDeleteMarker))
						{
							return existingEtag;
						}
					}

					throw new ConcurrencyException(method + " attempted on document '" + key +
												   "' using a non current etag")
					{
						ActualETag = existingEtag,
						ExpectedETag = etag
					};
				}
			}

			return existingEtag;
		}

		//returns true if it was update operation
		private bool WriteDocumentMetadata(JsonDocumentMetadata metadata,bool shouldIgnoreConcurrencyExceptions = false)
		{
            var metadataStream = CreateStream();

			metadataStream.Write(metadata.Etag);
			metadataStream.Write(metadata.Key);

			if (metadata.LastModified.HasValue)
				metadataStream.Write(metadata.LastModified.Value.ToBinary());
			else
				metadataStream.Write((long)0);

			metadata.Metadata.WriteTo(metadataStream);

			metadataStream.Position = 0;

			var loweredKey = CreateKey(metadata.Key);

			ushort? existingVersion;
			var isUpdate = metadataIndex.Contains(Snapshot, loweredKey, writeBatch.Value, out existingVersion);
			metadataIndex.Add(writeBatch.Value, loweredKey, metadataStream, existingVersion, shouldIgnoreConcurrencyExceptions);

			return isUpdate;
		}

		private JsonDocumentMetadata ReadDocumentMetadata(string key)
		{
			var loweredKey = CreateKey(key);

			var metadataReadResult = metadataIndex.Read(Snapshot, loweredKey, writeBatch.Value);
			if (metadataReadResult == null)
				return null;

			using (var stream = metadataReadResult.Reader.AsStream())
			{
				stream.Position = 0;
				var etag = stream.ReadEtag();
				var originalKey = stream.ReadString();
				var lastModifiedDateTimeBinary = stream.ReadInt64();

				var existingCachedDocument = documentCacher.GetCachedDocument(loweredKey, etag);

				var metadata = existingCachedDocument != null ? existingCachedDocument.Metadata : stream.ToJObject();
			    var lastModified = DateTime.FromBinary(lastModifiedDateTimeBinary);

				return new JsonDocumentMetadata
				{
					Key = originalKey,
					Etag = etag,
					Metadata = metadata,
					LastModified = lastModified
				};
			}
		}

		private bool WriteDocumentData(string key, Etag etag, RavenJObject data, RavenJObject metadata, out Etag newEtag, out Etag existingEtag, out DateTime savedAt)
		{
			var keyByEtagDocumentIndex = tableStorage.Documents.GetIndex(Tables.Documents.Indices.KeyByEtag);
			var loweredKey = CreateKey(key);

			ushort? existingVersion;
			var isUpdate = tableStorage.Documents.Contains(Snapshot, loweredKey, writeBatch.Value, out existingVersion);
			existingEtag = null;

			if (isUpdate)
			{
				existingEtag = EnsureDocumentEtagMatch(loweredKey, etag, "PUT");
				keyByEtagDocumentIndex.Delete(writeBatch.Value, existingEtag);
			}
			else if (etag != null && etag != Etag.Empty)
			{
				throw new ConcurrencyException("PUT attempted on document '" + key +
													   "' using a non current etag (document deleted)")
				{
					ExpectedETag = etag
				};
			}

            var dataStream = CreateStream();


			using (var finalDataStream = documentCodecs.Aggregate((Stream) new UndisposableStream(dataStream),
				(current, codec) => codec.Encode(loweredKey, data, metadata, current)))
			{
				data.WriteTo(finalDataStream);
				finalDataStream.Flush();
			}
 
			dataStream.Position = 0;
			tableStorage.Documents.Add(writeBatch.Value, loweredKey, dataStream, existingVersion); 

			newEtag = uuidGenerator.CreateSequentialUuid(UuidType.Documents);
			savedAt = SystemTime.UtcNow;

			var isUpdated = PutDocumentMetadataInternal(key, metadata, newEtag, savedAt);

			keyByEtagDocumentIndex.Add(writeBatch.Value, newEtag, loweredKey);

			return isUpdated;
		}

		private RavenJObject ReadDocumentData(string key, Etag existingEtag, RavenJObject metadata)
		{
			var loweredKey = CreateKey(key);

			var existingCachedDocument = documentCacher.GetCachedDocument(loweredKey, existingEtag);
			if (existingCachedDocument != null)
				return existingCachedDocument.Document;

			var documentReadResult = tableStorage.Documents.Read(Snapshot, loweredKey, writeBatch.Value);
			if (documentReadResult == null) //non existing document
				return null;

			using (var stream = documentReadResult.Reader.AsStream())
			{
				using (var decodedDocumentStream = documentCodecs.Aggregate(stream,
						(current, codec) => codec.Value.Decode(loweredKey, metadata, current)))
				{
					var documentData = decodedDocumentStream.ToJObject();

					documentCacher.SetCachedDocument(loweredKey, existingEtag, documentData, metadata, (int)stream.Length);

					return documentData;	
				}
			}
		}

		public DebugDocumentStats GetDocumentStatsVerySlowly()
		{
			//TODO : write implementation _before_ finishing merge of Voron stuff into 3.0
			throw new NotImplementedException();
		}
	}
}
