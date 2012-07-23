//-----------------------------------------------------------------------
// <copyright file="DocumentsStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.MEF;
using Raven.Database;
using Raven.Database.Impl;
using Raven.Database.Json;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Raven.Storage.Managed.Impl;
using System.Linq;

namespace Raven.Storage.Managed
{
	public class DocumentsStorageActions : IDocumentStorageActions
	{
		private readonly TableStorage storage;
		private readonly ITransactionStorageActions transactionStorageActions;
		private readonly IUuidGenerator generator;
		private readonly OrderedPartCollection<AbstractDocumentCodec> documentCodecs;
		private readonly IDocumentCacher documentCacher;

		public DocumentsStorageActions(TableStorage storage, 
			ITransactionStorageActions transactionStorageActions, 
			IUuidGenerator generator, 
			OrderedPartCollection<AbstractDocumentCodec> documentCodecs,
			IDocumentCacher documentCacher)
		{
			this.storage = storage;
			this.transactionStorageActions = transactionStorageActions;
			this.generator = generator;
			this.documentCodecs = documentCodecs;
		    this.documentCacher = documentCacher;
		}

		public Tuple<int, int> FirstAndLastDocumentIds()
		{
			var lastOrDefault = storage.Documents["ById"].LastOrDefault();
			var last = 0;
			if (lastOrDefault != null)
				last = lastOrDefault.Value<int>("id");

			var firstOrDefault = storage.Documents["ById"].FirstOrDefault();
			var first = 0;
			if (firstOrDefault != null)
				first= firstOrDefault.Value<int>("id");
			return new Tuple<int, int>(first,last );
		}

		public IEnumerable<JsonDocument> GetDocumentsByReverseUpdateOrder(int start, int take)
		{
			return storage.Documents["ByEtag"].SkipFromEnd(start)
				.Select(result => DocumentByKey(result.Value<string>("key"), null))
				.Take(take);
		}

		public IEnumerable<JsonDocument> GetDocumentsAfter(Guid etag, int take, long? maxSize = null)
		{
			var docs = storage.Documents["ByEtag"].SkipAfter(new RavenJObject {{"etag", etag.ToByteArray()}})
				.Select(result => DocumentByKey(result.Value<string>("key"), null))
				.Take(take);
			long totalSize = 0;
			foreach (var doc in docs)
			{
				totalSize += doc.SerializedSizeOnDisk;
				if (maxSize != null && totalSize > maxSize.Value)
				{
					yield return doc;
					yield break;
				}
				yield return doc;
			}
		}

		public IEnumerable<JsonDocument> GetDocumentsWithIdStartingWith(string idPrefix, int start, int take)
		{
			return storage.Documents["ByKey"].SkipTo(new RavenJObject{{"key", idPrefix }})
				.Skip(start)
				.TakeWhile(x => x.Value<string>("key").StartsWith(idPrefix))
				.Select(result => DocumentByKey(result.Value<string>("key"), null))
				.Take(take);
		}

		public long GetDocumentsCount()
		{
			return storage.Documents["ByKey"].Count;
		}

		public JsonDocument DocumentByKey(string key, TransactionInformation transactionInformation)
		{
			return DocumentByKeyInternal(key, transactionInformation, (tuple, metadata) => new JsonDocument
			{
				SerializedSizeOnDisk = tuple.Item3,
				Metadata = metadata.Metadata,
				Etag = metadata.Etag,
				Key = metadata.Key,
				LastModified = metadata.LastModified,
				NonAuthoritativeInformation = metadata.NonAuthoritativeInformation,
				DataAsJson = ReadDocument(tuple, metadata)
			});
		}

		public JsonDocumentMetadata DocumentMetadataByKey(string key, TransactionInformation transactionInformation)
		{
			return DocumentByKeyInternal(key, transactionInformation, (stream, metadata) => metadata);
		}


		private T DocumentByKeyInternal<T>(string key, TransactionInformation transactionInformation, Func<Tuple<MemoryStream, RavenJObject, int>, JsonDocumentMetadata, T> createResult)
			where T : class
		{
			RavenJObject metadata;

			var resultInTx = storage.DocumentsModifiedByTransactions.Read(new RavenJObject { { "key", key } });
			if (transactionInformation != null && resultInTx != null)
			{
				if(new Guid(resultInTx.Key.Value<byte[]>("txId")) == transactionInformation.Id)
				{
					if (resultInTx.Key.Value<bool>("deleted"))
						return null;

					var txEtag = new Guid(resultInTx.Key.Value<byte[]>("etag"));
					var resultTx = ReadMetadata(key, txEtag, resultInTx.Data, out metadata);
					return createResult(resultTx, new JsonDocumentMetadata
					{
						Key = resultInTx.Key.Value<string>("key"),
						Etag = txEtag,
						Metadata = metadata,
						LastModified = resultInTx.Key.Value<DateTime>("modified"),
					});
				}
			}

			var readResult = storage.Documents.Read(new RavenJObject { { "key", key } });
			if (readResult == null)
			{
				if(resultInTx != null)
				{
					return createResult(Tuple.Create<MemoryStream,RavenJObject, int>(null, new RavenJObject(), 0), new JsonDocumentMetadata
					{
						Key = resultInTx.Key.Value<string>("key"),
						Etag = Guid.Empty,
						Metadata = new RavenJObject { { Constants.RavenDocumentDoesNotExists, true } },
						NonAuthoritativeInformation = true,
						LastModified = DateTime.MinValue
					});
				}
				return null;
			}

			var etag = new Guid(readResult.Key.Value<byte[]>("etag"));
			var result = ReadMetadata(key, etag, readResult.Data, out metadata);
			return createResult(result, new JsonDocumentMetadata
			{
				Key = readResult.Key.Value<string>("key"),
				Etag = etag,
				Metadata = metadata,
				LastModified = readResult.Key.Value<DateTime>("modified"),
				NonAuthoritativeInformation = resultInTx != null
			});
		}

		private Tuple<MemoryStream, RavenJObject, int> ReadMetadata(string key, Guid etag, Func<byte[]> getData, out RavenJObject metadata)
		{
			var cachedDocument = documentCacher.GetCachedDocument(key, etag);
			if (cachedDocument != null)
			{
				metadata = cachedDocument.Metadata;
				return Tuple.Create<MemoryStream, RavenJObject,int>(null, cachedDocument.Document, cachedDocument.Size);
			}

			var buffer = getData();
			var memoryStream = new MemoryStream(buffer, 0, buffer.Length, true , true);

			metadata = memoryStream.ToJObject();

			return Tuple.Create<MemoryStream, RavenJObject, int>(memoryStream, null, (int)memoryStream.Length);
		}

		private RavenJObject ReadDocument(Tuple<MemoryStream, RavenJObject, int> stream, JsonDocumentMetadata metadata)
		{
			if (stream.Item2 != null)
				return stream.Item2;

			RavenJObject result;
			Stream docDataStream = stream.Item1;
			if (documentCodecs.Count() > 0)
			{
				var metadataCopy = (RavenJObject)metadata.Metadata.CloneToken() ;
				using (docDataStream = documentCodecs
					.ReverseAggregate(docDataStream, (dataStream, codec) => codec.Decode(metadata.Key, metadataCopy, dataStream)))
					result = docDataStream.ToJObject();
			}
			else
			{
				result = docDataStream.ToJObject();
			}

			Debug.Assert(metadata.Etag != null);
			documentCacher.SetCachedDocument(metadata.Key, metadata.Etag.Value, result, metadata.Metadata, stream.Item3);

			return result;
		}

		public bool DeleteDocument(string key, Guid? etag, out RavenJObject metadata)
		{
			var existingEtag = AssertValidEtag(key, etag, "DELETE", null);

			metadata = null;
			var readResult = storage.Documents.Read(new RavenJObject { { "key", key } });
			if (readResult == null)
				return false;

			metadata = readResult.Data().ToJObject();

			storage.Documents.Remove(new RavenJObject { { "key", key } });

			documentCacher.RemoveCachedDocument(key, existingEtag);

			return true;
		}

		public Guid AddDocument(string key, Guid? etag, RavenJObject data, RavenJObject metadata)
		{
			var existingEtag = AssertValidEtag(key, etag, "PUT", null);

			var ms = new MemoryStream();

			metadata.WriteTo(ms);

			using (var stream = documentCodecs.Aggregate<Lazy<AbstractDocumentCodec>, Stream>(ms,
				(dataStream, codec) => codec.Value.Encode(key, data, metadata, dataStream)))
			{
				data.WriteTo(stream);
				stream.Flush();
			}

			var newEtag = generator.CreateSequentialUuid();
			storage.Documents.Put(new RavenJObject
			 {
				 {"key", key},
				 {"etag", newEtag.ToByteArray()},
				 {"modified", SystemTime.UtcNow},
				 {"id", GetNextDocumentId()},
				 {"entityName", metadata.Value<string>(Constants.RavenEntityName)}
			 }, ms.ToArray());

			documentCacher.RemoveCachedDocument(key, existingEtag);

			return newEtag;
		}

		private int lastGeneratedId;
		private int GetNextDocumentId()
		{
			if (lastGeneratedId > 0)
				return ++lastGeneratedId;

			var lastOrefaultKeyById = storage.Documents["ById"].LastOrDefault();
			int id = 1;
			if (lastOrefaultKeyById != null)
				id = lastOrefaultKeyById.Value<int>("id") + 1;
			lastGeneratedId = id;
			return id;
		}

		private Guid AssertValidEtag(string key, Guid? etag, string op, TransactionInformation transactionInformation)
		{
			var readResult = storage.Documents.Read(new RavenJObject { { "key", key } });

			if (readResult != null)
			{
				StorageHelper.AssertNotModifiedByAnotherTransaction(storage, transactionStorageActions, key, readResult, transactionInformation);
				var existingEtag = new Guid(readResult.Key.Value<byte[]>("etag"));

				if (etag != null)
				{
					if (existingEtag != etag)
					{
						if(etag.Value == Guid.Empty)
						{
							RavenJObject metadata;
							ReadMetadata(key, existingEtag, readResult.Data, out metadata);
							if (metadata.ContainsKey(Constants.RavenDeleteMarker) && metadata.Value<bool>(Constants.RavenDeleteMarker))
							{
								return existingEtag;
							}
						}

						throw new ConcurrencyException(op + " attempted on document '" + key +
													   "' using a non current etag")
						{
							ActualETag = etag.Value,
							ExpectedETag = existingEtag
						};
					}
				}
				return existingEtag;
			}
			
			if (etag != null && etag != Guid.Empty) // expected something to be there.
				throw new ConcurrencyException("PUT attempted on document '" + key +
				                               "' using a non current etag (document deleted)")
				{
					ExpectedETag = etag.Value
				};

			readResult = storage.DocumentsModifiedByTransactions.Read(new RavenJObject { { "key", key } });
			StorageHelper.AssertNotModifiedByAnotherTransaction(storage, transactionStorageActions, key, readResult, transactionInformation);

			if(readResult == null)
				return Guid.Empty;

			return new Guid(readResult.Key.Value<byte[]>("etag"));
		}

	}
}
