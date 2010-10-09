using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Exceptions;
using Raven.Database.Storage;
using Raven.Database.Storage.StorageActions;
using Raven.Storage.Managed.Data;

namespace Raven.Storage.Managed
{
	public class DocumentStorageActions : AbstractStorageActions, IDocumentStorageActions, ITransactionStorageActions
	{
		public Tuple<int, int> FirstAndLastDocumentIds()
		{
			var min = Viewer.DocumentsById.GetLeftMost();
			var max = Viewer.DocumentsById.GetRightMost();

			return new Tuple<int, int>(
				min.Type == JTokenType.Null ? 0 : min.Value<int>(),
				max.Type == JTokenType.Null ? 0 : max.Value<int>()
				);
		}

		public IEnumerable<Tuple<JsonDocument, int>> DocumentsById(int startId, int endId)
		{
			foreach (var treeNode in Viewer.DocumentsById.ScanFromInclusive(startId))
			{
				var docId = treeNode.NodeKey.Value<int>();
				if (docId > endId)
					break;

				if (treeNode.NodeValue == null)
					continue;
				Reader.Position = treeNode.NodeValue.Value;
				var key = BinaryReader.ReadString();
				yield return new Tuple<JsonDocument, int>(
					DocumentByKey(key, null),
					docId);
			}
		}

		public IEnumerable<JsonDocument> GetDocumentsAfter(Guid etag)
		{
			return from treeNode in Viewer.DocumentsByEtag.ScanFromExclusive(etag.ToByteArray())
				   let docPos = treeNode.NodeValue
				   where docPos != null 
				   select ReadDocument(docPos.Value);
		}

		public IEnumerable<JsonDocument> GetDocumentsByReverseUpdateOrder(int start)
		{
			return (from treeNode in Viewer.DocumentsByEtag.ReverseScanFromInclusive(Guid.Empty.ToByteArray())
			        let docPos = treeNode.NodeValue
			        where docPos != null
			        select docPos.Value)
				.Skip(start)
				.Select(ReadDocument);
		}

		public IEnumerable<string> DocumentKeys
		{
			get
			{
				return Viewer.Documents.Keys.Select(key => key.Value<string>());
			}
		}

		public long GetDocumentsCount()
		{
			return Viewer.DocumentCount;
		}

		public JsonDocument DocumentByKey(string key, TransactionInformation transactionInformation)
		{
			if(transactionInformation != null)
			{
				var docInTxPos = Viewer.DocumentsInTransaction.FindValue(new JObject(
				                                                        	new JProperty("TxId", transactionInformation.Id.ToByteArray()),
				                                                        	new JProperty("Key", key)));
				if (docInTxPos != null)
					return ReadDocument(docInTxPos.Value);
			}
			var docPos = Viewer.Documents.FindValue(key);
			if (docPos == null)
				return null;

			return ReadDocument(docPos.Value);
		}

		private JsonDocument ReadDocument(long docPos)
		{
			Reader.Position = docPos;
			var key = BinaryReader.ReadString();
			var metadata = JObject.Load(new BsonReader(Reader));
			if (metadata.Value<bool>("@deleted_in_tx"))
				return null;
			var doc = JObject.Load(new BsonReader(Reader));

			return new JsonDocument
			{
				DataAsJson = doc,
				Etag = new Guid(metadata.Value<string>("@etag")),
				Key = key,
				Metadata = metadata,
				NonAuthoritiveInformation = false,
			};
		}

		public bool DeleteDocument(string key, Guid? etag, out JObject metadata)
		{
			var docPos = Mutator.Documents.FindValue(key);
			metadata = null;
			if (docPos == null)
				return false;
			var existingEtag = EnsureValidEtag(docPos.Value, etag);
			Mutator.DecrementDocumentCount();
			Reader.Position = docPos.Value;
			ConsumeKeyFromFile();
			metadata = JObject.Load(new BsonReader(Reader));
			var docId = metadata.Value<int>("@docId");
			Mutator.DocumentsById.Remove(docId);
			Mutator.DocumentsByEtag.Remove(new Guid(existingEtag).ToByteArray());
			Mutator.Documents.Remove(key);
			return true;
		}

		/// <summary>
		/// This function is used to skip over the key in the file, so we can get directly to the document
		/// metadata
		/// </summary>
		private string ConsumeKeyFromFile()
		{
			var keyFromFile = BinaryReader.ReadString();
			return keyFromFile;
		}

		public void DeleteDocumentInTransaction(TransactionInformation transactionInformation, string key, Guid? etag)
		{
			BeforeWorkingWithDocumentInsideTransaction(key, transactionInformation, etag);
			var pos = Writer.Position;
			BinaryWriter.Write(key);
			new JObject(
				new JProperty("@deleted_in_tx", true),
				new JProperty("@etag", DocumentDatabase.CreateSequentialUuid().ToString())
				).WriteTo(new BsonWriter(Writer));
			new JObject().WriteTo(new BsonWriter(Writer));
			Mutator.DocumentsInTransaction.Add(GetDocInTxKey(key, transactionInformation.Id), pos);
		}

		public Guid AddDocumentInTransaction(string key, Guid? etag, JObject data, JObject metadata, TransactionInformation transactionInformation)
		{
			BeforeWorkingWithDocumentInsideTransaction(key, transactionInformation, etag);

			metadata["@locking_tx"] = new JValue(transactionInformation.Id.ToString());
			var newEtag = DocumentDatabase.CreateSequentialUuid();
			long position = WriteDocument(key, metadata, newEtag, data);

			Mutator.DocumentsInTransaction.Add(GetDocInTxKey(key, transactionInformation.Id),position);
			return newEtag;
		}

		private static JObject GetDocInTxKey(string key, Guid txId)
		{
			return new JObject(
				new JProperty("TxId", txId.ToByteArray()),
				new JProperty("Key", key)
				);
		}

		private void BeforeWorkingWithDocumentInsideTransaction(string key, TransactionInformation transactionInformation, Guid? etag)
		{
			EnsureNotLockedByAnotherTransaction(key, transactionInformation.Id);
			var docPos = Mutator.Documents.FindValue(key);
			EnsureDocumentEtagMatchInTransaction(transactionInformation.Id, key, docPos, etag);
			if (docPos != null)
			{
				EnsureDocumentLockedInTransaction(transactionInformation.Id, docPos.Value);
			}
			EnsureTransactionExists(transactionInformation);
		}

		private void EnsureTransactionExists(TransactionInformation transactionInformation)
		{
			var txKey = transactionInformation.Id.ToByteArray();
			var timeout = DateTime.UtcNow.Add(transactionInformation.Timeout);
			var pos = Writer.Position;
			BinaryWriter.Write(timeout.ToBinary());
			Mutator.Transactions.Add(txKey, pos);
		}

		private void EnsureDocumentLockedInTransaction(Guid txId, long docPos)
		{
			Reader.Position = docPos;
			var key = ConsumeKeyFromFile();
			var metadata = JObject.Load(new BsonReader(Reader));
			var bytes = metadata.Value<byte[]>("@locking_tx");
			if (bytes != null && new Guid(bytes) == txId)
				return;// already locked
			var doc = JObject.Load(new BsonReader(Reader));
			AddDocument(key, null, doc, metadata);
		}

		private void EnsureDocumentEtagMatchInTransaction(Guid txId, string key, long? docPos, Guid? etag)
		{
			if (etag == null)
				return;
			var docInTxPos = Mutator.DocumentsInTransaction.FindValue(GetDocInTxKey(key, txId));
			var documentPosition = docInTxPos ?? docPos;
			if (documentPosition == null)
				return;
			EnsureValidEtag(documentPosition.Value, etag);
		}

		private void EnsureNotLockedByAnotherTransaction(string key, Guid txId)
		{
			var docinTxNode = Mutator.DocumentsInTransaction.FindNode(new JObject(new JProperty("Key", JValue.CreateString(key))));
			if (docinTxNode == null)
				return;

			var existingTxId = new Guid(docinTxNode.NodeKey.Value<byte[]>("TxId"));
			if (existingTxId == txId) // current transaction, it is okay
			{
				return;
			}
			var txNode = Mutator.Transactions.FindValue(existingTxId.ToByteArray());
			if (txNode == null) // probably a bug, resetting TX 
			{
				RollbackTransaction(existingTxId);
				return;
			}

			Reader.Position = txNode.Value;
			var timeout = DateTime.FromBinary(BinaryReader.ReadInt64());
			if (DateTime.UtcNow > timeout) // the timeout for the transaction has passed, we can roll it back
			{
				RollbackTransaction(existingTxId);
				return;
			}

			throw new ConcurrencyException("A document with key: '" + key + "' is currently created in another transaction");
		}

		public void RollbackTransaction(Guid txId)
		{
			Mutator.Transactions.Remove(txId.ToByteArray());
			foreach (var treeNode in GetNodesInTransaction(txId))
			{
				Mutator.DocumentsInTransaction.Remove(treeNode.NodeKey);
			}
		}

		public void ModifyTransactionId(Guid fromTxId, Guid toTxId, TimeSpan timeout)
		{
			var transactionInformation = new TransactionInformation
			{
				Id = toTxId,
				Timeout = timeout
			};
			EnsureTransactionExists(transactionInformation);
			CompleteTransaction(fromTxId, doc =>
			{
				var docPos = Mutator.Documents.FindValue(doc.Key);
				if (docPos != null)
				{
					EnsureDocumentLockedInTransaction(transactionInformation.Id, docPos.Value);
				}
				if (doc.Delete)
					DeleteDocumentInTransaction(transactionInformation, doc.Key, null);
				else
					AddDocumentInTransaction(doc.Key, null, doc.Data, doc.Metadata, transactionInformation);
			});
		}

		public void CompleteTransaction(Guid txId, Action<DocumentInTransactionData> perDocumentModified)
		{
			Mutator.Transactions.Remove(txId.ToByteArray());
			foreach (var node in GetNodesInTransaction(txId))
			{
				Mutator.DocumentsInTransaction.Remove(node.NodeKey);
				if(node.NodeValue == null)
					continue;
				Reader.Position = node.NodeValue.Value;
				var key = BinaryReader.ReadString();
				var metadata = JObject.Load(new BsonReader(Reader));
				var doc = JObject.Load(new BsonReader(Reader));
				perDocumentModified(new DocumentInTransactionData
				{
					Delete = metadata.Value<bool>("@deleted_in_tx"),
					Key = key,
					Etag = new Guid(metadata.Value<string>("@etag")),
					Metadata = metadata,
					Data = doc
				});
			}
		}

		private IEnumerable<TreeNode> GetNodesInTransaction(Guid txId)
		{
			return Mutator.DocumentsInTransaction.ScanFromInclusive(new JObject(new JProperty("TxId", new JValue(txId.ToByteArray()))))
				.TakeWhile(node => new Guid(node.NodeKey.Value<byte[]>("TxId")) == txId);
		}

		public Guid AddDocument(string key, Guid? etag, JObject data, JObject metadata)
		{
			var docPos = Mutator.Documents.FindValue(key);

			EnsureNotLockedByAnotherTransaction(key, Guid.Empty);
			
			if (docPos == null)
			{
				var max = Mutator.DocumentsById.GetRightMost();
				var current = max.Type == JTokenType.Null ? 0 : max.Value<int>();
				var docId = current + 1;
				metadata["@docId"] = new JValue(docId);
				var docKeyPos = Writer.Position;
				BinaryWriter.Write(key);
				Mutator.DocumentsById.Add(docId, docKeyPos);
				Mutator.IncrementDocumentCount();
			}
			else
			{
				var oldEtag = EnsureValidEtag(docPos.Value, etag);
				Mutator.DocumentsByEtag.Remove(oldEtag);
			}
			var newEtag = DocumentDatabase.CreateSequentialUuid();
			long position = WriteDocument(key, metadata, newEtag, data);
			Mutator.Documents.Add(key, position);
			Mutator.DocumentsByEtag.Add(newEtag.ToByteArray(), position);
			return newEtag;
		}

		private long WriteDocument(string key, JObject metadata, Guid newEtag, JObject data)
		{
			var position = Writer.Position;
			metadata["@etag"] = new JValue(newEtag.ToString());
			BinaryWriter.Write(key);
			metadata.WriteTo(new BsonWriter(Writer));
			data.WriteTo(new BsonWriter(Writer));
			return position;
		}

		private byte[] EnsureValidEtag(long documentPosition, Guid? etag)
		{
			Reader.Position = documentPosition;
			var key = BinaryReader.ReadString();
			var storedHeaders = JObject.Load(new BsonReader(Reader));
			var existingEtag = new Guid(storedHeaders.Value<string>("@etag"));

			if (etag == null)
				return existingEtag.ToByteArray();

			if (existingEtag != etag.Value)
			{
				throw new ConcurrencyException("PUT attempted on document '" + key +
					"' using a non current etag")
				{
					ActualETag = etag.Value,
					ExpectedETag = existingEtag
				};
			}
			return existingEtag.ToByteArray();
		}


		public IEnumerable<Guid> GetTransactionIds()
		{
			return from node in Viewer.Transactions.ScanFromInclusive(Guid.Empty.ToByteArray())
				   select new Guid(node.NodeKey.Value<byte[]>());
		}

	}
}