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
            CreateSchema();
        }

	    public SnapshotReader CreateSnapshot()
	    {
	        return env.CreateSnapshot();
	    }	

		public Table Details { get; private set; }

        public Table Documents { get; private set; }

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
				CreateDetailsSchema(tx);
				CreateDocumentsSchema(tx);

                //TODO : add trees creation code here as needed - when accessors are added to StorageActionAccessor class 

                tx.Commit();
			}
		}

		private void CreateDetailsSchema(Transaction tx)
		{
			env.CreateTree(tx, Tables.Details.TableName);
		}

		private void CreateDocumentsSchema(Transaction tx)
		{
			env.CreateTree(tx, Tables.Documents.TableName);
			env.CreateTree(tx, Documents.GetIndexKey(Tables.Documents.Indices.KeyByEtag));
		}

	    private void Initialize()
	    {
            Documents = new Table(Tables.Documents.TableName, Tables.Documents.Indices.KeyByEtag);
            Details = new Table(Tables.Details.TableName);
	    }
	}
}