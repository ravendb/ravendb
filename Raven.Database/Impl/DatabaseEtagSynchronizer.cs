// -----------------------------------------------------------------------
//  <copyright file="DatabaseEtagSynchronizer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
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

		public Etag CalculateSynchronizationEtagFor(Expression<Func<EtagSynchronizationContext, Etag>> propertySelector, Expression<Func<EtagSynchronizationContext, Etag>> synchronizationPropertySelector, Etag currentEtag, Etag lastProcessedEtag)
		{
			if (currentEtag == null)
			{
				if (lastProcessedEtag != null)
				{
					lock (locker)
					{
						var etag = GetValue(propertySelector);
						var synchronizationEtag = GetValue(synchronizationPropertySelector);
						if (etag == null && lastProcessedEtag.CompareTo(synchronizationEtag) != 0)
						{
							SetValue(synchronizationPropertySelector, lastProcessedEtag);
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

		public Etag GetSynchronizationEtagFor(Expression<Func<EtagSynchronizationContext, Etag>> propertySelector, Expression<Func<EtagSynchronizationContext, Etag>> synchronizationPropertySelector)
		{
			lock (locker)
			{
				var etag = GetValue(propertySelector);

				if (etag != null)
				{
					PersistSynchronizationContext();
					SetValue(synchronizationPropertySelector, etag);
					ResetSynchronizationEtagFor(propertySelector);
				}

				return etag;
			}
		}

		private void ResetSynchronizationEtagFor(Expression<Func<EtagSynchronizationContext, Etag>> propertySelector)
		{
			SetValue(propertySelector, null);
		}

		private void PersistSynchronizationContext()
		{
			var indexerEtag = GetEtagForPersistance(x => x.IndexerEtag, x => x.LastIndexerSynchronizedEtag);
			var reducerEtag = GetEtagForPersistance(x => x.ReducerEtag, x => x.LastReducerSynchronizedEtag);
			var replicatorEtag = GetEtagForPersistance(x => x.ReplicatorEtag, x => x.LastReplicatorSynchronizedEtag);
			var sqlReplicatorEtag = GetEtagForPersistance(x => x.SqlReplicatorEtag, x => x.LastSqlReplicatorSynchronizedEtag);

			transactionalStorage.Batch(actions => actions.Staleness.PutSynchronizationContext(indexerEtag, reducerEtag, replicatorEtag, sqlReplicatorEtag));
		}

		private Etag GetEtagForPersistance(Expression<Func<EtagSynchronizationContext, Etag>> propertySelector,
										   Expression<Func<EtagSynchronizationContext, Etag>> synchronizationPropertySelector)
		{
			var etag = GetValue(propertySelector);
			var synchronizationEtag = GetValue(synchronizationPropertySelector);

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
				shouldPersist |= UpdateSynchronizationContextFor(x => x.IndexerEtag, x => x.LastIndexerSynchronizedEtag, lowestEtag);
				shouldPersist |= UpdateSynchronizationContextFor(x => x.ReducerEtag, x => x.LastReducerSynchronizedEtag, lowestEtag);
				shouldPersist |= UpdateSynchronizationContextFor(x => x.ReplicatorEtag, x => x.LastReplicatorSynchronizedEtag, lowestEtag);
				shouldPersist |= UpdateSynchronizationContextFor(x => x.SqlReplicatorEtag, x => x.LastSqlReplicatorSynchronizedEtag, lowestEtag);

				if (shouldPersist)
					PersistSynchronizationContext();
			}
		}

		private bool UpdateSynchronizationContextFor(Expression<Func<EtagSynchronizationContext, Etag>> propertySelector, Expression<Func<EtagSynchronizationContext, Etag>> synchronizationPropertySelector, Etag lowestEtag)
		{
			var etag = GetValue(propertySelector);
			var synchronizationEtag = GetValue(synchronizationPropertySelector);

			if (etag == null || lowestEtag.CompareTo(etag) < 0)
			{
				SetValue(propertySelector, lowestEtag);
			}

			if (lowestEtag.CompareTo(synchronizationEtag) < 0)
				return true;

			return false;
		}

		private TType GetValue<TClass, TType>(Expression<Func<TClass, TType>> expression) where TType : class
		{
			var prop = (PropertyInfo)((MemberExpression)expression.Body).Member;
			return prop.GetValue(context, null) as TType;
		}

		private void SetValue<TClass, TType>(Expression<Func<TClass, TType>> expression, TType value) where TType : class
		{
			var prop = (PropertyInfo)((MemberExpression)expression.Body).Member;
			prop.SetValue(context, value, null);
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

	public class EtagSynchronizationContext
	{
		public EtagSynchronizationContext()
		{
			IndexerEtag = Etag.Empty;
			LastIndexerSynchronizedEtag = Etag.Empty;
			ReducerEtag = Etag.Empty;
			LastReducerSynchronizedEtag = Etag.Empty;
			ReplicatorEtag = Etag.Empty;
			LastReplicatorSynchronizedEtag = Etag.Empty;
			SqlReplicatorEtag = Etag.Empty;
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