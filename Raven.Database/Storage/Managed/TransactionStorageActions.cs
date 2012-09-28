//-----------------------------------------------------------------------
// <copyright file="TransactionStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.MEF;
using Raven.Database.Impl;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Raven.Munin;
using Raven.Storage.Managed.Impl;
using System.Linq;
using Raven.Database.Json;

namespace Raven.Storage.Managed
{
	public class TransactionStorageActions : ITransactionStorageActions
	{
		private readonly TableStorage storage;
		private readonly IUuidGenerator generator;
		private readonly OrderedPartCollection<AbstractDocumentCodec> documentCodecs;

		public TransactionStorageActions(TableStorage storage, IUuidGenerator generator, OrderedPartCollection<AbstractDocumentCodec> documentCodecs)
		{
			this.storage = storage;
			this.generator = generator;
			this.documentCodecs = documentCodecs;
		}

		public Guid AddDocumentInTransaction(string key, Guid? etag, RavenJObject data, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			var readResult = storage.Documents.Read(new RavenJObject { { "key", key } });
			if (readResult != null) // update
			{
				StorageHelper.AssertNotModifiedByAnotherTransaction(storage, this, key, readResult, transactionInformation);
				AssertValidEtag(key, readResult, storage.DocumentsModifiedByTransactions.Read(new RavenJObject { { "key", key } }), etag, "DELETE");

				var ravenJObject = ((RavenJObject)readResult.Key.CloneToken());
				ravenJObject["txId"] = transactionInformation.Id.ToByteArray();
				if (storage.Documents.UpdateKey(ravenJObject) == false)
					throw new ConcurrencyException("PUT attempted on document '" + key +
												   "' that is currently being modified by another transaction");
			}
			else
			{
				readResult = storage.DocumentsModifiedByTransactions.Read(new RavenJObject { { "key", key } });
				StorageHelper.AssertNotModifiedByAnotherTransaction(storage, this, key, readResult, transactionInformation);
			}

			storage.Transactions.UpdateKey(new RavenJObject
			                               	{
			                               		{"txId", transactionInformation.Id.ToByteArray()},
			                               		{"timeout", SystemTime.UtcNow.Add(transactionInformation.Timeout)}
			                               	});

			var ms = new MemoryStream();

			metadata.WriteTo(ms);
			using (var stream = documentCodecs.Aggregate<Lazy<AbstractDocumentCodec>, Stream>(ms, (memoryStream, codec) => codec.Value.Encode(key, data, metadata, memoryStream)))
			{
				data.WriteTo(stream);
				stream.Flush();
			}
			var newEtag = generator.CreateSequentialUuid();
			storage.DocumentsModifiedByTransactions.Put(new RavenJObject
			                                            	{
			                                            		{"key", key},
			                                            		{"etag", newEtag.ToByteArray()},
			                                            		{"modified", SystemTime.UtcNow},
			                                            		{"txId", transactionInformation.Id.ToByteArray()}
			                                            	}, ms.ToArray());

			return newEtag;
		}

		private static void AssertValidEtag(string key, Table.ReadResult doc, Table.ReadResult docInTx, Guid? etag, string operation)
		{
			if (doc == null)
				return;
			var existingEtag =
				docInTx != null
					? new Guid(docInTx.Key.Value<byte[]>("etag"))
					: new Guid(doc.Key.Value<byte[]>("etag"));


			if (etag != null && etag.Value != existingEtag)
			{
				throw new ConcurrencyException(operation + " attempted on document '" + key +
											   "' using a non current etag")
				{
					ActualETag = existingEtag,
					ExpectedETag = etag.Value
				};
			}
		}

		public bool DeleteDocumentInTransaction(TransactionInformation transactionInformation, string key, Guid? etag)
		{
			var nonTxResult = storage.Documents.Read(new RavenJObject { { "key", key } });
			if (nonTxResult == null)
			{

				if (etag != null && etag.Value != Guid.Empty)
				{
					throw new ConcurrencyException("DELETE attempted on document '" + key +
												   "' using a non current etag")
					{
						ActualETag = Guid.Empty,
						ExpectedETag = etag.Value
					};
				}
				return false;
			}

			var readResult = storage.DocumentsModifiedByTransactions.Read(new RavenJObject { { "key", key } });
			StorageHelper.AssertNotModifiedByAnotherTransaction(storage, this, key, readResult, transactionInformation);
			AssertValidEtag(key, nonTxResult, readResult, etag, "DELETE");

			if (readResult != null)
			{
				var ravenJObject = ((RavenJObject)readResult.Key.CloneToken());
				ravenJObject["txId"] = transactionInformation.Id.ToByteArray();
				if (storage.Documents.UpdateKey(readResult.Key) == false)
					throw new ConcurrencyException("DELETE attempted on document '" + key +
												   "' that is currently being modified by another transaction");
			}

			storage.Transactions.UpdateKey(new RavenJObject
				{
					{"txId", transactionInformation.Id.ToByteArray()},
					{"timeout", SystemTime.UtcNow.Add(transactionInformation.Timeout)}
				});

			var newEtag = generator.CreateSequentialUuid();
			storage.DocumentsModifiedByTransactions.UpdateKey(new RavenJObject
				{
					{"key", key},
					{"etag", newEtag.ToByteArray()},
					{"modified", SystemTime.UtcNow},
					{"deleted", true},
					{"txId", transactionInformation.Id.ToByteArray()}
				});

			return true;
		}

		public void RollbackTransaction(Guid txId)
		{
			CompleteTransaction(txId, data =>
			{
				var readResult = storage.Documents.Read(new RavenJObject { { "key", data.Key } });
				if (readResult == null)
					return;
				var ravenJObject = ((RavenJObject)readResult.Key.CloneToken());
				ravenJObject.Remove("txId");
				storage.Documents.UpdateKey(readResult.Key);
			});
		}

		public void ModifyTransactionId(Guid fromTxId, Guid toTxId, TimeSpan timeout)
		{
			storage.Transactions.UpdateKey(new RavenJObject
			{
				{"txId", toTxId.ToByteArray()},
				{"timeout", SystemTime.UtcNow.Add(timeout)}
			});

			var transactionInformation = new TransactionInformation { Id = toTxId, Timeout = timeout };
			CompleteTransaction(fromTxId, data =>
			{
				var readResult = storage.Documents.Read(new RavenJObject { { "key", data.Key } });
				if (readResult != null)
				{
					var ravenJObject = ((RavenJObject)readResult.Key.CloneToken());
					ravenJObject["txId"] = toTxId.ToByteArray();
					storage.Documents.UpdateKey(readResult.Key);
				}

				if (data.Delete)
					DeleteDocumentInTransaction(transactionInformation, data.Key, null);
				else
					AddDocumentInTransaction(data.Key, null, data.Data, data.Metadata, transactionInformation);
			});
		}

		public bool TransactionExists(Guid txId)
		{
			return storage.Transactions.Read(new RavenJObject { { "txId", txId.ToByteArray() } }) != null;
		}

		public void CompleteTransaction(Guid txId, Action<DocumentInTransactionData> perDocumentModified)
		{
			storage.Transactions.Remove(new RavenJObject { { "txId", txId.ToByteArray() } });

			var documentsInTx = storage.DocumentsModifiedByTransactions["ByTxId"]
				.SkipTo(new RavenJObject { { "txId", txId.ToByteArray() } })
				.TakeWhile(x => new Guid(x.Value<byte[]>("txId")) == txId);

			foreach (var docInTx in documentsInTx)
			{
				var readResult = storage.DocumentsModifiedByTransactions.Read(docInTx);

				storage.DocumentsModifiedByTransactions.Remove(docInTx);

				RavenJObject metadata = null;
				RavenJObject data = null;
				if (readResult.Position > 0) // position can never be 0, because of the skip record
				{
					var ms = new MemoryStream(readResult.Data());
					metadata = ms.ToJObject();
					data = ms.ToJObject();
				}
				perDocumentModified(new DocumentInTransactionData
				{
					Key = readResult.Key.Value<string>("key"),
					Etag = new Guid(readResult.Key.Value<byte[]>("etag")),
					Delete = readResult.Key.Value<bool>("deleted"),
					Metadata = metadata,
					Data = data,
				});

			}
		}

		public IEnumerable<Guid> GetTransactionIds()
		{
			return storage.Transactions.Keys.Select(x => new Guid(x.Value<byte[]>("txId")));
		}
	}
}
