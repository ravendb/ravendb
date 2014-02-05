// -----------------------------------------------------------------------
//  <copyright file="MuninInFlightTransactionalState.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Raven.Storage.Managed;

namespace Raven.Database.Impl.DTC
{
	public class MuninInFlightTransactionalState : InFlightTransactionalState
	{
		private readonly TransactionalStorage storage;

		public MuninInFlightTransactionalState(TransactionalStorage storage, Func<string, Etag, RavenJObject, RavenJObject, TransactionInformation, PutResult> databasePut, Func<string, Etag, TransactionInformation, bool> databaseDelete) : base(databasePut, databaseDelete)
		{
			this.storage = storage;
		}

		public override void Commit(string id)
		{
			storage.Batch(accessor => RunOperationsInTransaction(id));
		}
		public override void Prepare(string id)
		{
			// nothing to do here - Munin does not support doing work in the Prepare phase
		}
	}
}