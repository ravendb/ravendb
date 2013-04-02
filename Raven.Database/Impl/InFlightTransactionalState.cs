// -----------------------------------------------------------------------
//  <copyright file="InFlightTransactionalState.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Database.Impl
{
	public class InFlightTransactionalState
	{
		private class TransactionState
		{
			public readonly List<DocumentInTransactionData> changes = new List<DocumentInTransactionData>();
			public volatile Reference<DateTime> lastSeen = new Reference<DateTime>();
		}

		private class ChangedDoc
		{
			public Guid transactionId;
			public Etag currentEtag;
			public Etag committedEtag;
		}

		readonly ConcurrentDictionary<string, ChangedDoc> changedInTransaction = new ConcurrentDictionary<string, ChangedDoc>();

		private readonly ConcurrentDictionary<Guid, TransactionState> transactionStates =
			new ConcurrentDictionary<Guid, TransactionState>();

		public Etag AddDocumentInTransaction(
			string key,
			Etag etag,
			RavenJObject data,
			RavenJObject metadata,
			TransactionInformation transactionInformation,
			Etag committedEtag,
			SequentialUuidGenerator uuidGenerator)
		{
			metadata.EnsureCannotBeChangeAndEnableSnapshotting();
			data.EnsureCannotBeChangeAndEnableSnapshotting();
			return AddToTransactionState(key, etag,
								  transactionInformation,
								  committedEtag,
								  new DocumentInTransactionData
								  {
									  Metadata = metadata,
									  Data = data,
									  Delete = false,
									  Key = key,
									  LastModified = SystemTime.UtcNow,
									  Etag = uuidGenerator.CreateSequentialUuid(UuidType.DocumentTransactions)
								  });
		}

		public void DeleteDocumentInTransaction(
			TransactionInformation transactionInformation,
			string key,
			Etag etag,
			Etag committedEtag,
			SequentialUuidGenerator uuidGenerator)
		{
			AddToTransactionState(key, etag, transactionInformation, committedEtag, new DocumentInTransactionData
			{
				Delete = true,
				Key = key,
				LastModified = SystemTime.UtcNow
			});
		}

		public bool IsModified(string key)
		{
			var value = currentlyCommittingTransaction.Value;
			if (value == Guid.Empty)
				return changedInTransaction.ContainsKey(key);
			ChangedDoc doc;
			if (changedInTransaction.TryGetValue(key, out doc) == false)
				return false;
			return doc.transactionId != value;
		}

		public Func<TDocument, TDocument> GetNonAuthoritativeInformationBehavior<TDocument>(TransactionInformation tx, string key) where TDocument : class, IJsonDocumentMetadata, new()
		{
			ChangedDoc existing;
			if (changedInTransaction.TryGetValue(key, out existing) == false || (tx != null && tx.Id == existing.transactionId))
				return null;

			TransactionState value;
			if (transactionStates.TryGetValue(existing.transactionId, out value) == false ||
				SystemTime.UtcNow > value.lastSeen.Value)
			{
				Rollback(existing.transactionId);
				return null;
			}
			return document =>
			{
				if (document == null)
				{
					return new TDocument
					{
						Key = key,
						Metadata = new RavenJObject {{Constants.RavenDocumentDoesNotExists, true}},
						LastModified = DateTime.MinValue,
						NonAuthoritativeInformation = true,
						Etag = Etag.Empty
					};
				}

				document.NonAuthoritativeInformation = true;
				return document;
			};
		}

		public void Rollback(Guid id)
		{
			TransactionState value;
			if (transactionStates.TryGetValue(id, out value) == false)
				return;
			lock (value)
			{
				foreach (var change in value.changes)
				{
					ChangedDoc guid;
					changedInTransaction.TryRemove(change.Key, out guid);
				}
			}

			transactionStates.TryRemove(id, out value);
		}

		private readonly ThreadLocal<Guid> currentlyCommittingTransaction = new ThreadLocal<Guid>();

		public void Commit(Guid id, Action<DocumentInTransactionData> action)
		{
			TransactionState value;
			if (transactionStates.TryGetValue(id, out value) == false)
				throw new InvalidOperationException("There is no transaction with id: " + id);

			lock (value)
			{
				currentlyCommittingTransaction.Value = id;
				try
				{
					foreach (var change in value.changes)
					{
						action(new DocumentInTransactionData
						{
							Metadata = change.Metadata == null ? null : (RavenJObject)change.Metadata.CreateSnapshot(),
							Data = change.Data == null ? null : (RavenJObject)change.Data.CreateSnapshot(),
							Delete = change.Delete,
							Etag = change.Etag,
							LastModified = change.LastModified,
							Key = change.Key
						});
					}
				}
				finally
				{
					currentlyCommittingTransaction.Value = Guid.Empty;
				}
			}
		}

		private Etag AddToTransactionState(string key,
			Etag etag,
			TransactionInformation transactionInformation,
			Etag committedEtag,
			DocumentInTransactionData item)
		{
			try
			{
				var state = transactionStates.GetOrAdd(transactionInformation.Id, id => new TransactionState());
				lock (state)
				{
					state.lastSeen = new Reference<DateTime>
					{
						Value = SystemTime.UtcNow + transactionInformation.Timeout
					};

					var currentTxVal = state.changes.LastOrDefault(x => string.Equals(x.Key, key, StringComparison.InvariantCultureIgnoreCase));
					if (currentTxVal != null)
					{
						if (etag != null && currentTxVal.Etag != etag)
							throw new ConcurrencyException("Transaction operation attempted on : " + key + " using a non current etag");
						state.changes.Remove(currentTxVal);
					}
					var result = changedInTransaction.AddOrUpdate(key, s =>
					{
						if (etag != null && etag != committedEtag)
							throw new ConcurrencyException("Transaction operation attempted on : " + key + " using a non current etag");

						return new ChangedDoc
						{
							transactionId = transactionInformation.Id,
							committedEtag = committedEtag,
							currentEtag = item.Etag
						};
					}, (_, existing) =>
					{
						if (existing.transactionId == transactionInformation.Id)
						{
							if (etag != null && etag != existing.currentEtag)
								throw new ConcurrencyException("Transaction operation attempted on : " + key + " using a non current etag");

							return existing;
						}

						TransactionState transactionState;
						if (transactionStates.TryGetValue(existing.transactionId, out transactionState) == false ||
							SystemTime.UtcNow > transactionState.lastSeen.Value)
						{
							Rollback(existing.transactionId);

							if (etag != null && etag != committedEtag)
								throw new ConcurrencyException("Transaction operation attempted on : " + key + " using a non current etag");

							return new ChangedDoc
							{
								transactionId = transactionInformation.Id,
								committedEtag = committedEtag,
								currentEtag = item.Etag
							};
						}

						throw new ConcurrencyException("Document " + key + " is being modified by another transaction: " + existing);
					});


					state.changes.Add(item);

					return result.currentEtag;
				}
			}
			catch (Exception)
			{
				Rollback(transactionInformation.Id);
				throw;
			}
		}

		public bool TryGet(string key, TransactionInformation transactionInformation, out JsonDocument document)
		{
			return TryGetInternal(key, transactionInformation, (theKey, change) => new JsonDocument
			{
				DataAsJson = (RavenJObject)change.Data.CreateSnapshot(),
				Metadata = (RavenJObject)change.Metadata.CreateSnapshot(),
				Key = theKey,
				Etag = change.Etag,
				NonAuthoritativeInformation = false,
				LastModified = change.LastModified
			}, out document);
		}

		public bool TryGet(string key, TransactionInformation transactionInformation, out JsonDocumentMetadata document)
		{
			return TryGetInternal(key, transactionInformation, (theKey, change) => new JsonDocumentMetadata
			{
				Metadata = (RavenJObject)change.Metadata.CreateSnapshot(),
				Key = theKey,
				Etag = change.Etag,
				NonAuthoritativeInformation = false,
				LastModified = change.LastModified
			}, out document);
		}

		private bool TryGetInternal<T>(string key, TransactionInformation transactionInformation, Func<string, DocumentInTransactionData, T> createDoc, out T document)
			where T : class
		{
			TransactionState state;
			if (transactionStates.TryGetValue(transactionInformation.Id, out state) == false)
			{
				document = null;
				return false;
			}
			var change = state.changes.LastOrDefault(x => string.Equals(x.Key, key, StringComparison.InvariantCultureIgnoreCase));
			if (change == null)
			{
				document = null;
				return false;
			}
			if (change.Delete)
			{
				document = null;
				return true;
			}
			document = createDoc(key, change);
			return true;
		}

		public bool HasTransaction(Guid txId)
		{
			return transactionStates.ContainsKey(txId);
		}
	}
}