//-----------------------------------------------------------------------
// <copyright file="StorageHelper.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Raven.Munin;
using Raven.Storage.Managed.Impl;

namespace Raven.Storage.Managed
{
	public static class StorageHelper
	{
		public static void AssertNotModifiedByAnotherTransaction(TableStorage storage, ITransactionStorageActions transactionStorageActions, string key, Table.ReadResult readResult, TransactionInformation transactionInformation)
		{
			if (readResult == null)
				return;
			var txIdAsBytes = readResult.Key.Value<byte[]>("txId");
			if (txIdAsBytes == null)
				return;

			var txId = new Guid(txIdAsBytes);
			if (transactionInformation != null && transactionInformation.Id == txId)
			{
				return;
			}

			var existingTx = storage.Transactions.Read(new RavenJObject { { "txId", txId.ToByteArray() } });
			if (existingTx == null)//probably a bug, ignoring this as not a real tx
				return;

			var timeout = existingTx.Key.Value<DateTime>("timeout");
			if (SystemTime.UtcNow > timeout)
			{
				transactionStorageActions.RollbackTransaction(txId);
				return;
			}

			throw new ConcurrencyException("Document '" + key + "' is locked by transaction: " + txId);
		}
	}
}