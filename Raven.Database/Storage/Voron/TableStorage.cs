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
        private const string DocumentsTreeName = "documents";
        private const string DocumentsKeyByEtagIndexName = "key_by_etag";

	    private readonly IPersistanceSource persistanceSource;

		private readonly StorageEnvironment env;

		public TableStorage(IPersistanceSource persistanceSource)
		{
			this.persistanceSource = persistanceSource;
			env = new StorageEnvironment(persistanceSource.Pager, ownsPager: false);

            Initialize();
            CreateSchema();
        }

	    public SnapshotReader CreateSnapshot()
	    {
	        return env.CreateSnapshot();
	    }	

		public Table Details { get; private set; }

        public IndexedTable Documents { get; private set; }

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

        //create all relevant storage trees in one place
		private void CreateSchema()
		{
			using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
			{
			    env.CreateTree(tx, DetailsTreeName);

			    env.CreateTree(tx, DocumentsTreeName);
                
			    env.CreateTree(tx, Documents.GetIndexKey(DocumentsKeyByEtagIndexName));

                //TODO : add trees creation code here as needed - when accessors are added to StorageActionAccessor class 

                tx.Commit();
			}
		}

	    private void Initialize()
	    {
            Documents = new IndexedTable(DocumentsTreeName, DocumentsKeyByEtagIndexName);
            Details = new Table(DetailsTreeName);
	    }
	}
}