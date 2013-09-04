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

		public Table Tasks { get; private set; }

		public Table ScheduledReductions { get; private set; }

		public Table MappedResults { get; private set; }

		public Table ReduceResults { get; private set; }

		public Table Attachments { get; private set; }

		public Table ReduceKeys { get; private set; }

		public void Write(WriteBatch writeBatch)
		{
			env.Writer.Write(writeBatch);
		}

		public long GetEntriesCount(TableBase table)
		{
			using (var tx = env.NewTransaction(TransactionFlags.Read))
			{
				return env.GetTree(tx, table.TableName).State.EntriesCount;
			}
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
				CreateTasksSchema(tx);
				CreateStalenessSchema(tx);
				CreateScheduledReductionsSchema(tx);
				CreateMappedResultsSchema(tx);
				CreateAttachmentsSchema(tx);
				CreateReduceKeysSchema(tx);
				CreateReduceResultsSchema(tx);

				tx.Commit();
			}
		}

		private void CreateReduceResultsSchema(Transaction tx)
		{
			env.CreateTree(tx, Tables.ReduceResults.TableName);
		}

		private void CreateReduceKeysSchema(Transaction tx)
		{
			env.CreateTree(tx, Tables.ReduceKeys.TableName);
			env.CreateTree(tx, ReduceKeys.GetIndexKey(Tables.ReduceKeys.Indices.ByView));
		}

		private void CreateAttachmentsSchema(Transaction tx)
		{
			env.CreateTree(tx, Tables.Attachments.TableName);
			env.CreateTree(tx, Attachments.GetIndexKey(Tables.Attachments.Indices.ByEtag));
		}

		private void CreateMappedResultsSchema(Transaction tx)
		{
			env.CreateTree(tx, Tables.MappedResults.TableName);
		}

		private void CreateScheduledReductionsSchema(Transaction tx)
		{
			env.CreateTree(tx, Tables.ScheduledReductions.TableName);
			env.CreateTree(tx, ScheduledReductions.GetIndexKey(Tables.ScheduledReductions.Indices.ByView));
			env.CreateTree(tx, ScheduledReductions.GetIndexKey(Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey));
		}

		private void CreateStalenessSchema(Transaction tx)
		{
		}

		private void CreateTasksSchema(Transaction tx)
		{
			env.CreateTree(tx, Tables.Tasks.TableName);
			env.CreateTree(tx, Tasks.GetIndexKey(Tables.Tasks.Indices.ByIndexAndType));
			env.CreateTree(tx, Tasks.GetIndexKey(Tables.Tasks.Indices.ByType));
			env.CreateTree(tx, Tasks.GetIndexKey(Tables.Tasks.Indices.ByIndex));
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
			Tasks = new Table(Tables.Tasks.TableName, Tables.Tasks.Indices.ByIndexAndType, Tables.Tasks.Indices.ByType, Tables.Tasks.Indices.ByIndex);
			ScheduledReductions = new Table(Tables.ScheduledReductions.TableName, Tables.ScheduledReductions.Indices.ByView, Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey);
			MappedResults = new Table(Tables.MappedResults.TableName);
			ReduceKeys = new Table(Tables.ReduceKeys.TableName, Tables.ReduceKeys.Indices.ByView);
			Attachments = new Table(Tables.Attachments.TableName);
			ReduceResults = new Table(Tables.ReduceResults.TableName);
		}
	}
}