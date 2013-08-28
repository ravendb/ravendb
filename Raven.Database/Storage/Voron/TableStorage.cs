// -----------------------------------------------------------------------
//  <copyright file="TableStorage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Database.Tasks;

namespace Raven.Database.Storage.Voron
{
	using System;

	using global::Voron;
	using global::Voron.Impl;

	public class TableStorage : IDisposable
	{
	    private const string DetailsTreeName = "details";
	    private readonly IPersistanceSource persistanceSource;

		private readonly StorageEnvironment env;

		public TableStorage(IPersistanceSource persistanceSource)
		{
			this.persistanceSource = persistanceSource;
			env = new StorageEnvironment(persistanceSource.Pager, ownsPager: false);

			Initialize();
		}

	    public SnapshotReader CreateSnapshot()
	    {
	        return env.CreateSnapshot();
	    }	

		public Table Details { get; private set; }

	    public void Write(WriteBatch writeBatch)
	    {
	        env.Writer.Write(writeBatch);
	    }

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
			    env.CreateTree(tx, DetailsTreeName);
                Details = new Table(DetailsTreeName);
                tx.Commit();
			}
		}
	}
}