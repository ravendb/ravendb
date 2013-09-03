// -----------------------------------------------------------------------
//  <copyright file="TableStorage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Storage.Voron.Impl
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

		public Table IndexingStats { get; private set; }

		public Table LastIndexedEtags { get; private set; }

		public Table DocumentReferences { get; private set; }

		public Table Queues { get; private set; }

		public Table Lists { get; private set; }

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
				CreateIndexingStatsSchema(tx);
				CreateLastIndexedEtagsSchema(tx);
				CreateDocumentReferencesSchema(tx);
				CreateQueuesSchema(tx);
				CreateListsSchema(tx);

                //TODO : add trees creation code here as needed - when accessors are added to StorageActionAccessor class 

                tx.Commit();
			}
		}

		private void CreateListsSchema(Transaction tx)
		{
			env.CreateTree(tx, Tables.Lists.TableName);
			env.CreateTree(tx, Lists.GetIndexKey(Tables.Lists.Indices.ByName));
			env.CreateTree(tx, Lists.GetIndexKey(Tables.Lists.Indices.ByNameAndKey));
		}

		private void CreateQueuesSchema(Transaction tx)
		{
			env.CreateTree(tx, Tables.Queues.TableName);
			env.CreateTree(tx, Queues.GetIndexKey(Tables.Queues.Indices.ByName));
		}

		private void CreateDocumentReferencesSchema(Transaction tx)
		{
			env.CreateTree(tx, Tables.DocumentReferences.TableName);
			env.CreateTree(tx, DocumentReferences.GetIndexKey(Tables.DocumentReferences.Indices.ByRef));
			env.CreateTree(tx, DocumentReferences.GetIndexKey(Tables.DocumentReferences.Indices.ByView));
			env.CreateTree(tx, DocumentReferences.GetIndexKey(Tables.DocumentReferences.Indices.ByViewAndKey));
			env.CreateTree(tx, DocumentReferences.GetIndexKey(Tables.DocumentReferences.Indices.ByKey));
		}

		private void CreateLastIndexedEtagsSchema(Transaction tx)
		{
			env.CreateTree(tx, Tables.LastIndexedEtags.TableName);
		}

		private void CreateIndexingStatsSchema(Transaction tx)
		{
			env.CreateTree(tx, Tables.IndexingStats.TableName);
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
			IndexingStats = new Table(Tables.IndexingStats.TableName);
			LastIndexedEtags = new Table(Tables.LastIndexedEtags.TableName);
			DocumentReferences = new Table(Tables.DocumentReferences.TableName, Tables.DocumentReferences.Indices.ByRef, Tables.DocumentReferences.Indices.ByView, Tables.DocumentReferences.Indices.ByViewAndKey, Tables.DocumentReferences.Indices.ByKey);
			Queues = new Table(Tables.Queues.TableName, Tables.Queues.Indices.ByName);
			Lists = new Table(Tables.Lists.TableName, Tables.Lists.Indices.ByName, Tables.Lists.Indices.ByNameAndKey);
	    }
	}
}