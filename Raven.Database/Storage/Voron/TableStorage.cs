// -----------------------------------------------------------------------
//  <copyright file="TableStorage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Storage.Voron
{
	using System;

	using global::Voron;
	using global::Voron.Impl;

	public class TableStorage : IDisposable
	{
		private readonly IPersistanceSource persistanceSource;

		private readonly StorageEnvironment env;

		public TableStorage(IPersistanceSource persistanceSource)
		{
			this.persistanceSource = persistanceSource;
			env = new StorageEnvironment(persistanceSource.Pager, ownsPager: false);

			Initialize();
		}

		public Transaction NewTransaction(TransactionFlags flags)
		{
			return env.NewTransaction(flags);
		}

		public Table Details { get; private set; }

		public void Dispose()
		{
			if (persistanceSource != null)
				persistanceSource.Dispose();

			if (env != null)
				env.Dispose();
		}

		private void Initialize()
		{
			using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Details = new Table(env.CreateTree(tx, "details"));
			}
		}
	}
}