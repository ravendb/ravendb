// -----------------------------------------------------------------------
//  <copyright file="InFlightTransactionalState.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Json.Linq;

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
			return AddToTransactionState(key, etag,
			                      transactionInformation,
			                      committedEtag,
			                      uuidGenerator,
			                      new DocumentInTransactionData
			                      {
				                      Metadata = metadata,
				                      Data = data,
				                      Delete = false,
				                      Key = key
			                      });
		}

		public void DeleteDocumentInTransaction(
			TransactionInformation transactionInformation,
			string key,
			Etag etag,
			Etag committedEtag,
			SequentialUuidGenerator uuidGenerator)
		{
			AddToTransactionState(key, etag, transactionInformation, committedEtag, uuidGenerator, new DocumentInTransactionData
			{
				Delete = true,
				Key = key
			});
		}

		public bool IsModified(string key)
		{
			return changedInTransaction.ContainsKey(key);
		}

		public TDocument SetNonAuthoritativeInformation<TDocument>(TransactionInformation tx, string key, TDocument document) where TDocument : class, IJsonDocumentMetadata, new()
		{
			ChangedDoc existing;
			if (changedInTransaction.TryGetValue(key, out existing) == false || (tx != null && tx.Id == existing.transactionId))
				return document;

			TransactionState value;
			if (transactionStates.TryGetValue(existing.transactionId, out value) == false ||
				SystemTime.UtcNow > value.lastSeen.Value)
			{
				Rollback(existing.transactionId);
				return document;
			}

			if (document == null)
			{
				return new TDocument
				{
					Key = key,
					Metadata = new RavenJObject { { Constants.RavenDocumentDoesNotExists, true } },
					LastModified = DateTime.MinValue,
					NonAuthoritativeInformation = true,
					Etag = Etag.Empty
				};
			}

			document.NonAuthoritativeInformation = true;
			return document;
		}

		public void Rollback(Guid id)
		{
			TransactionState value;
			if (transactionStates.TryRemove(id, out value) == false)
				return;
			lock (value)
			{
				foreach (var change in value.changes)
				{
					ChangedDoc guid;
					changedInTransaction.TryRemove(change.Key, out guid);
				}
			}
		}

		public void Commit(Guid id, Action<DocumentInTransactionData> action)
		{
			TransactionState value;
			if (transactionStates.TryRemove(id, out value) == false)
				throw new InvalidOperationException("There is no transaction with id: " + id);

			lock (value)
			{
				foreach (var change in value.changes)
				{
					action(change);
					ChangedDoc guid;
					changedInTransaction.TryRemove(change.Key, out guid);
				}
			}
		}

		private Etag AddToTransactionState(string key,
			Etag etag,
			TransactionInformation transactionInformation,
			Etag committedEtag,
			SequentialUuidGenerator uuidGenerator,
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
					
					var result = changedInTransaction.AddOrUpdate(key, s =>
					{
						if (etag != null && etag != committedEtag)
							throw new ConcurrencyException("Transaction operation attempted on : " + key + " using a non current etag");
							
						return new ChangedDoc
						{
							transactionId = transactionInformation.Id,
							committedEtag = committedEtag,
							currentEtag = uuidGenerator.CreateSequentialUuid(UuidType.DocumentTransactions)
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
								currentEtag = uuidGenerator.CreateSequentialUuid(UuidType.DocumentTransactions)
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
	}
}