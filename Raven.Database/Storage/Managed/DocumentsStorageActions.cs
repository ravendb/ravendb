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
using Raven.Abstractions.Util;
using Raven.Database.Impl;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Raven.Munin;
using Raven.Storage.Managed.Impl;
using System.Linq;

namespace Raven.Storage.Managed
{
	public class DocumentsStorageActions : IDocumentStorageActions
	{
		private readonly TableStorage storage;
		private readonly IUuidGenerator generator;
		private readonly OrderedPartCollection<AbstractDocumentCodec> documentCodecs;
		private readonly IDocumentCacher documentCacher;

		private readonly Dictionary<Etag, Etag> etagTouches = new Dictionary<Etag, Etag>();

		public DocumentsStorageActions(TableStorage storage,
			IUuidGenerator generator,
			OrderedPartCollection<AbstractDocumentCodec> documentCodecs,
			IDocumentCacher documentCacher)
		{
			this.storage = storage;
			this.generator = generator;
			this.documentCodecs = documentCodecs;
			this.documentCacher = documentCacher;
		}

		public IEnumerable<JsonDocument> GetDocumentsByReverseUpdateOrder(int start, int take)
		{
			return storage.Documents["ByEtag"].SkipFromEnd(start)
				.Select(result => DocumentByKey(result.Value<string>("key"), null))
				.Take(take);
		}

		public IEnumerable<JsonDocument> GetDocumentsAfter(Etag etag, int take, long? maxSize = null, Etag untilEtag = null)
		{
			var docs = storage.Documents["ByEtag"].SkipAfter(new RavenJObject { { "etag", etag.ToByteArray() } })
				.Select(result => DocumentByKey(result.Value<string>("key"), null))
				.Take(take);
			long totalSize = 0;
			int count = 0;
			foreach (var doc in docs)
			{
				totalSize += doc.SerializedSizeOnDisk;
				if (maxSize != null && totalSize > maxSize.Value)
				{
					yield return doc;
					yield break;
				}
				if (untilEtag != null && count > 0)
				{
					if (EtagUtil.IsGreaterThanOrEqual(doc.Etag, untilEtag))
						yield break;
				}
				count++;
				yield return doc;
			}
		}

		public Etag GetBestNextDocumentEtag(Etag etag)
		{
			var match = storage.Documents["ByEtag"].SkipAfter(new RavenJObject { { "etag", etag.ToByteArray() } })
												  .FirstOrDefault();
			if (match == null)
				return etag;
			return Etag.Parse(match.Value<byte[]>("etag"));
		}

	    public DebugDocumentStats GetDocumentStatsVerySlowly()
	    {
	        var sp = Stopwatch.StartNew();
	        var stat = new DebugDocumentStats {Total = GetDocumentsCount()};
            foreach (var readResult in storage.Documents)
            {
                var key = readResult.Key.Value<string>("key");
                if (key.StartsWith("Raven/", StringComparison.OrdinalIgnoreCase))
                    stat.System++;

                var metadata = readResult.Data().ToJObject();

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
			return storage.Documents["ByKey"].SkipTo(new RavenJObject { { "key", idPrefix } })
				.Skip(start)
				.TakeWhile(x => x.Value<string>("key").StartsWith(idPrefix, StringComparison.OrdinalIgnoreCase))
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
				if (resultInTx.Key.Value<string>("txId") == transactionInformation.Id)
				{
					if (resultInTx.Key.Value<bool>("deleted"))
						return null;

					var txEtag = Etag.Parse(resultInTx.Key.Value<byte[]>("etag"));
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
				if (resultInTx != null)
				{
					return createResult(Tuple.Create<MemoryStream, RavenJObject, int>(null, new RavenJObject(), 0), new JsonDocumentMetadata
					{
						Key = resultInTx.Key.Value<string>("key"),
						Etag = Etag.Empty,
						Metadata = new RavenJObject { { Constants.RavenDocumentDoesNotExists, true } },
						NonAuthoritativeInformation = true,
						LastModified = DateTime.MinValue
					});
				}
				return null;
			}

			var etag = Etag.Parse(readResult.Key.Value<byte[]>("etag"));
			var result = ReadMetadata(key, etag, readResult.Data, out metadata);
			return createResult(result, new JsonDocumentMetadata
			{
				Key = readResult.Key.Value<string>("key"),
				Etag = etag,
				Metadata = metadata,
				LastModified = readResult.Key.Value<DateTime>("modified")
			});
		}

		private Tuple<MemoryStream, RavenJObject, int> ReadMetadata(string key, Etag etag, Func<byte[]> getData, out RavenJObject metadata)
		{
			var cachedDocument = documentCacher.GetCachedDocument(key, etag);
			if (cachedDocument != null)
			{
				metadata = cachedDocument.Metadata;
				return Tuple.Create<MemoryStream, RavenJObject, int>(null, cachedDocument.Document, cachedDocument.Size);
			}

			var buffer = getData();
			var memoryStream = new MemoryStream(buffer, 0, buffer.Length, true, true);

			metadata = memoryStream.ToJObject();

			return Tuple.Create<MemoryStream, RavenJObject, int>(memoryStream, null, (int)memoryStream.Length);
		}

		private RavenJObject ReadDocument(Tuple<MemoryStream, RavenJObject, int> stream, JsonDocumentMetadata metadata)
		{
			if (stream.Item2 != null)
				return stream.Item2;

			RavenJObject result;
			Stream docDataStream = stream.Item1;
			if (documentCodecs.Any())
			{
				var metadataCopy = (RavenJObject)metadata.Metadata.CloneToken();
				using (docDataStream = documentCodecs
					.Aggregate(docDataStream, (dataStream, codec) => codec.Decode(metadata.Key, metadataCopy, dataStream)))
					result = docDataStream.ToJObject();
			}
			else
			{
				result = docDataStream.ToJObject();
			}

			Debug.Assert(metadata.Etag != null);
			documentCacher.SetCachedDocument(metadata.Key, metadata.Etag, result, metadata.Metadata, stream.Item3);

			return result;
		}

		public bool DeleteDocument(string key, Etag etag, out RavenJObject metadata, out Etag deletedETag)
		{
			var existingEtag = AssertValidEtag(key, etag, "DELETE", null);
			deletedETag = existingEtag;
			metadata = null;
			var readResult = storage.Documents.Read(new RavenJObject { { "key", key } });
			if (readResult == null)
				return false;

			metadata = readResult.Data().ToJObject();


			storage.Documents.Remove(new RavenJObject { { "key", key } });

			documentCacher.RemoveCachedDocument(key, existingEtag ?? Etag.Empty);

			return true;
		}

		public AddDocumentResult PutDocumentMetadata(string key, RavenJObject metadata)
		{
			var documentByKey = DocumentByKey(key, null);
			return AddDocument(key, documentByKey.Etag, documentByKey.DataAsJson, metadata);
		}

		public void TouchDocument(string key, out Etag preTouchEtag, out Etag afterTouchEtag)
		{
			var documentByKey = DocumentByKey(key, null);
			if (documentByKey == null)
			{
				preTouchEtag = null;
				afterTouchEtag = null;
				return;
			}
			var addDocumentResult = AddDocument(key, documentByKey.Etag, documentByKey.DataAsJson, documentByKey.Metadata);
			preTouchEtag = documentByKey.Etag;
			afterTouchEtag = addDocumentResult.Etag;

			etagTouches.Add(preTouchEtag, afterTouchEtag);
		}

		public AddDocumentResult InsertDocument(string key, RavenJObject data, RavenJObject metadata, bool checkForUpdates)
		{
			var ms = new MemoryStream();

			metadata.WriteTo(ms);

			using (var stream = documentCodecs.Aggregate<Lazy<AbstractDocumentCodec>, Stream>(ms,
				(dataStream, codec) => codec.Value.Encode(key, data, metadata, dataStream)))
			{
				data.WriteTo(stream);
				stream.Flush();
			}

			var readResult = storage.Documents.Read(new RavenJObject {{"key", key}});
			var isUpdate = readResult != null;

			if (isUpdate && checkForUpdates == false)
				throw new InvalidOperationException("Cannot insert document " + key + " because it already exists");

			Etag existingEtag = null;
			if(isUpdate)
				existingEtag = Etag.Parse(readResult.Key.Value<byte[]>("etag"));

			var newEtag = generator.CreateSequentialUuid(UuidType.Documents);
			var savedAt = SystemTime.UtcNow;
			storage.Documents.Put(new RavenJObject
			 {
				 {"key", key},
				 {"etag", newEtag.ToByteArray()},
				 {"modified", savedAt},
				 {"id", GetNextDocumentId()},
				 {"entityName", metadata.Value<string>(Constants.RavenEntityName)}
			 }, ms.ToArray());

			IncrementDocumentCount(1);
			return new AddDocumentResult
			{
				Etag = newEtag,
				PrevEtag = existingEtag,
				SavedAt = savedAt,
				Updated = isUpdate
			};
		}

		public void IncrementDocumentCount(int value)
		{
			// nothing to do here
		}

		public AddDocumentResult AddDocument(string key, Etag etag, RavenJObject data, RavenJObject metadata)
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

			var isUpdate = storage.Documents.Read(new RavenJObject { { "key", key } }) != null;

			var newEtag = generator.CreateSequentialUuid(UuidType.Documents);
			var savedAt = SystemTime.UtcNow;
			storage.Documents.Put(new RavenJObject
			 {
				 {"key", key},
				 {"etag", newEtag.ToByteArray()},
				 {"modified", savedAt},
				 {"id", GetNextDocumentId()},
				 {"entityName", metadata.Value<string>(Constants.RavenEntityName)}
			 }, ms.ToArray());

			documentCacher.RemoveCachedDocument(key, existingEtag ?? Etag.Empty);

			return new AddDocumentResult
			{
				Etag = newEtag,
				PrevEtag = existingEtag,
				SavedAt = savedAt,
				Updated = isUpdate
			};
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

		private Etag AssertValidEtag(string key, Etag etag, string op, TransactionInformation transactionInformation)
		{
			var readResult = storage.Documents.Read(new RavenJObject { { "key", key } });

			if (readResult != null)
			{
				var existingEtag = Etag.Parse(readResult.Key.Value<byte[]>("etag"));

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
							ActualETag = etag,
							ExpectedETag = existingEtag
						};
					}
				}
				return existingEtag;
			}

			if (etag != null && etag != Etag.Empty) // expected something to be there.
				throw new ConcurrencyException("PUT attempted on document '" + key +
											   "' using a non current etag (document deleted)")
				{
					ExpectedETag = etag
				};

			readResult = storage.DocumentsModifiedByTransactions.Read(new RavenJObject { { "key", key } });

			if (readResult == null)
				return null;

			return Etag.Parse(readResult.Key.Value<byte[]>("etag"));
		}
	}
}
