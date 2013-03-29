// -----------------------------------------------------------------------
//  <copyright file="TransactionalState.cs" company="Hibernating Rhinos LTD">
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
using Raven.Database.Util;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Database.Impl
{
	public class TransactionalState
	{
		private class State
		{
			// this is the only part that can be accessed concurrently
			public volatile bool Compeleted;

			public DateTime TimingOutAt;
			public HashSet<string> ModifiedDocs = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
		}

		private readonly ConcurrentDictionary<string, Guid> documentsModifiedByTransaction =
			new ConcurrentDictionary<string, Guid>(StringComparer.InvariantCulture);

		private readonly ConcurrentDictionary<Guid, State> txStatus = new ConcurrentDictionary<Guid, State>();

		public void DocumentModifiedByTransation(string key, Guid txId)
		{
			documentsModifiedByTransaction.AddOrUpdate(key, txId, (_, current) =>
			{
				State value;
				if (txStatus.TryGetValue(current, out value) == false) // transaction doesn't exists
					return txId;

				if (value.Compeleted) // transaction already completed
					return txId;

				if (SystemTime.UtcNow >= value.TimingOutAt)
				{
					DeleteTransaction(current);
					return txId;
				}

				throw new ConcurrencyException("Document " + key + " is locked by transaction: " + current);
			});

			State state;
			if (txStatus.TryGetValue(txId, out state) == false)
				throw new InvalidOperationException("Cannot call DocumentModifiedByTransation outside SerializeTransactionFor scope");
			state.ModifiedDocs.Add(key);
		}

		public void DeleteTransaction(Guid txId)
		{
			State value;
			if (txStatus.TryGetValue(txId, out value) == false)
				return;
			value.Compeleted = true;

			foreach (var doc in value.ModifiedDocs)
			{
				Guid _;
				documentsModifiedByTransaction.TryRemove(doc, out _);
			}
		}

		private DateTime lastTransactionCleanup;

		// this method is kept small to ensure that it can easily be inlined
		public IDisposable SerializeTransactionFor(TransactionInformation information)
		{
			if (information == null)
				return null;

			return ActuallyLockTransaction(information);
		}

		private IDisposable ActuallyLockTransaction(TransactionInformation information)
		{
			if (txStatus.Count > 256)
			{
				TryCleanup();
			}

			var state = txStatus.GetOrAdd(information.Id, id => new State());
			state.TimingOutAt = SystemTime.UtcNow + information.Timeout;

			Monitor.Enter(state);

			return new DisposableAction(() => Monitor.Exit(state));
		}

		private void TryCleanup()
		{
			var time = SystemTime.UtcNow;
			if (time > lastTransactionCleanup + TimeSpan.FromMinutes(1))
				return;
			lock (this)
			{
				if (time > lastTransactionCleanup + TimeSpan.FromMinutes(1))
					return;
				foreach (var source in txStatus.Where(x => time > x.Value.TimingOutAt))
				{
					DeleteTransaction(source.Key);
				}
				lastTransactionCleanup = SystemTime.UtcNow;
			}
		}

		public bool IsModified(string key)
		{
			return documentsModifiedByTransaction.ContainsKey(key);
		}

		public bool HasTransaction(Guid txId)
		{
			return txStatus.ContainsKey(txId);
		}
	}
}