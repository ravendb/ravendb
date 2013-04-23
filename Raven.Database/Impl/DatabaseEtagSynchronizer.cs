// -----------------------------------------------------------------------
//  <copyright file="DatabaseEtagSynchronizer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Database.Storage;

namespace Raven.Database.Impl
{
	public class DatabaseEtagSynchronizer
	{
		private readonly object locker = new object();

		private EtagSynchronizationContext context;

		private readonly ITransactionalStorage transactionalStorage;

		public DatabaseEtagSynchronizer(ITransactionalStorage transactionalStorage)
		{
			this.transactionalStorage = transactionalStorage;
			GetSynchronizationContext();
		}

		public void UpdateSynchronizationState(JsonDocument[] docs)
		{
			if (docs == null)
				return;

			var lowestEtag = GetLowestEtag(docs);

			UpdateSynchronizationContext(lowestEtag);
		}

		public Etag CalculateSynchronizationEtagFor(EtagSynchronizationType type, Etag currentEtag, Etag lastProcessedEtag)
		{
			if (currentEtag == null)
			{
				if (lastProcessedEtag != null)
				{
					lock (locker)
					{
						var etag = GetEtag(type);
						var synchronizationEtag = GetSynchronizationEtag(type);
						if (etag == null && lastProcessedEtag.CompareTo(synchronizationEtag) != 0)
						{
							SetSynchronizationEtag(type, lastProcessedEtag);
							PersistSynchronizationContext();
						}
					}

					return lastProcessedEtag;
				}

				return Etag.Empty;
			}

			if (lastProcessedEtag == null)
				return Etag.Empty;

			if (currentEtag.CompareTo(lastProcessedEtag) < 0)
				return currentEtag;

			return lastProcessedEtag;
		}

		public Etag GetSynchronizationEtagFor(EtagSynchronizationType type)
		{
			lock (locker)
			{
				var etag = GetEtag(type);

				if (etag != null)
				{
					PersistSynchronizationContext();
					SetSynchronizationEtag(type, etag);
					ResetSynchronizationEtagFor(type);
				}

				return etag;
			}
		}

		private void ResetSynchronizationEtagFor(EtagSynchronizationType type)
		{
			SetEtag(type, null);
		}

		private void PersistSynchronizationContext()
		{
			var indexerEtag = GetEtagForPersistance(EtagSynchronizationType.Indexer);
			var reducerEtag = GetEtagForPersistance(EtagSynchronizationType.Reducer);
			var replicatorEtag = GetEtagForPersistance(EtagSynchronizationType.Replicator);
			var sqlReplicatorEtag = GetEtagForPersistance(EtagSynchronizationType.SqlReplicator);

			transactionalStorage.Batch(actions => actions.Staleness.PutSynchronizationContext(indexerEtag, reducerEtag, replicatorEtag, sqlReplicatorEtag));
		}

		private Etag GetEtagForPersistance(EtagSynchronizationType type)
		{
			var etag = GetEtag(type);
			var synchronizationEtag = GetSynchronizationEtag(type);

			Etag result;
			if (etag != null)
			{
				result = etag.CompareTo(synchronizationEtag) < 0
								  ? etag
								  : synchronizationEtag;
			}
			else
			{
				result = synchronizationEtag;
			}

			return result ?? Etag.Empty;
		}

		private void GetSynchronizationContext()
		{
			transactionalStorage.Batch(actions =>
			{
				context = actions.Staleness.GetSynchronizationContext() ?? new EtagSynchronizationContext();
			});
		}

		private void UpdateSynchronizationContext(Etag lowestEtag)
		{
			lock (locker)
			{
				var shouldPersist = false;
				shouldPersist |= UpdateSynchronizationContextFor(EtagSynchronizationType.Indexer, lowestEtag);
				shouldPersist |= UpdateSynchronizationContextFor(EtagSynchronizationType.Reducer, lowestEtag);
				shouldPersist |= UpdateSynchronizationContextFor(EtagSynchronizationType.Replicator, lowestEtag);
				shouldPersist |= UpdateSynchronizationContextFor(EtagSynchronizationType.SqlReplicator, lowestEtag);

				if (shouldPersist)
					PersistSynchronizationContext();
			}
		}

		private bool UpdateSynchronizationContextFor(EtagSynchronizationType type, Etag lowestEtag)
		{
			var etag = GetEtag(type);
			var synchronizationEtag = GetSynchronizationEtag(type);

			if (etag == null || lowestEtag.CompareTo(etag) < 0)
			{
				SetEtag(type, lowestEtag);
			}

			if (lowestEtag.CompareTo(synchronizationEtag) < 0)
				return true;

			return false;
		}

		private Etag GetEtag(EtagSynchronizationType type)
		{
			switch (type)
			{
				case EtagSynchronizationType.Indexer:
					return context.IndexerEtag;
				case EtagSynchronizationType.Reducer:
					return context.ReducerEtag;
				case EtagSynchronizationType.Replicator:
					return context.ReplicatorEtag;
				case EtagSynchronizationType.SqlReplicator:
					return context.SqlReplicatorEtag;
				default:
					throw new NotSupportedException("type");
			}
		}

		private Etag GetSynchronizationEtag(EtagSynchronizationType type)
		{
			switch (type)
			{
				case EtagSynchronizationType.Indexer:
					return context.LastIndexerSynchronizedEtag;
				case EtagSynchronizationType.Reducer:
					return context.LastReducerSynchronizedEtag;
				case EtagSynchronizationType.Replicator:
					return context.LastReplicatorSynchronizedEtag;
				case EtagSynchronizationType.SqlReplicator:
					return context.LastSqlReplicatorSynchronizedEtag;
				default:
					throw new NotSupportedException("type");
			}
		}

		private void SetEtag(EtagSynchronizationType type, Etag value)
		{
			switch (type)
			{
				case EtagSynchronizationType.Indexer:
					context.IndexerEtag = value;
					break;
				case EtagSynchronizationType.Reducer:
					context.ReducerEtag = value;
					break;
				case EtagSynchronizationType.Replicator:
					context.ReplicatorEtag = value;
					break;
				case EtagSynchronizationType.SqlReplicator:
					context.SqlReplicatorEtag = value;
					break;
				default:
					throw new NotSupportedException("type");
			}
		}

		private void SetSynchronizationEtag(EtagSynchronizationType type, Etag value)
		{
			switch (type)
			{
				case EtagSynchronizationType.Indexer:
					context.LastIndexerSynchronizedEtag = value;
					break;
				case EtagSynchronizationType.Reducer:
					context.LastReducerSynchronizedEtag = value;
					break;
				case EtagSynchronizationType.Replicator:
					context.LastReplicatorSynchronizedEtag = value;
					break;
				case EtagSynchronizationType.SqlReplicator:
					context.LastSqlReplicatorSynchronizedEtag = value;
					break;
				default:
					throw new NotSupportedException("type");
			}
		}

		private static Etag GetLowestEtag(IList<JsonDocument> documents)
		{
			if (documents == null || documents.Count == 0)
				return Etag.Empty;

			var lowest = documents[0].Etag;

			for (var i = 1; i < documents.Count; i++)
			{
				var etag = documents[i].Etag;
				if (lowest.CompareTo(etag) <= 0)
					continue;

				lowest = etag;
			}

			return lowest;
		}
	}

	public enum EtagSynchronizationType
	{
		Indexer = 1,
		Reducer = 2,
		Replicator = 3,
		SqlReplicator = 4
	}

	public class EtagSynchronizationContext
	{
		public EtagSynchronizationContext()
		{
			LastIndexerSynchronizedEtag = Etag.Empty;
			LastReducerSynchronizedEtag = Etag.Empty;
			LastReplicatorSynchronizedEtag = Etag.Empty;
			LastSqlReplicatorSynchronizedEtag = Etag.Empty;
		}

		public Etag IndexerEtag { get; set; }

		public Etag LastIndexerSynchronizedEtag { get; set; }

		public Etag ReducerEtag { get; set; }

		public Etag LastReducerSynchronizedEtag { get; set; }

		public Etag ReplicatorEtag { get; set; }

		public Etag LastReplicatorSynchronizedEtag { get; set; }

		public Etag SqlReplicatorEtag { get; set; }

		public Etag LastSqlReplicatorSynchronizedEtag { get; set; }
	}
}