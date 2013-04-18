// -----------------------------------------------------------------------
//  <copyright file="DatabaseEtagSynchronizer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using Raven.Abstractions.Data;

namespace Raven.Database.Impl
{
	public class DatabaseEtagSynchronizer
	{
		private readonly object locker = new object();

		private readonly EtagSynchronizationContext context;

		public DatabaseEtagSynchronizer()
		{
			context = GetSynchronizationContext();
		}

		public void UpdateSynchronizationState(JsonDocument[] docs)
		{
			if (docs == null)
				return;

			var lowestEtag = GetLowestEtag(docs);

			UpdateSynchronizationContext(lowestEtag);
		}

		public Etag CalculateSynchronizationEtagFor(Etag currentEtag, Etag lastProcessedEtag)
		{
			if (currentEtag == null)
				return lastProcessedEtag ?? Etag.Empty;

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
					SetValue(synchronizationPropertySelector, etag);

				PersistSynchronizationContext();

				ResetSynchronizationEtagFor(propertySelector);

				return etag;
			}
		}

		private void ResetSynchronizationEtagFor(Expression<Func<EtagSynchronizationContext, Etag>> propertySelector)
		{
			lock (locker)
			{
				SetValue(propertySelector, null);
			}
		}

		private void PersistSynchronizationContext()
		{
		}

		private EtagSynchronizationContext GetSynchronizationContext()
		{
			return new EtagSynchronizationContext();
		}

		private void UpdateSynchronizationContext(Etag lowestEtag)
		{
			lock (locker)
			{
				var shouldPersist = false;
				shouldPersist |= UpdateSynchronizationContextFor(x => x.IndexerEtag, x => x.LastIndexerSynchronizedEtag, lowestEtag);
				shouldPersist |= UpdateSynchronizationContextFor(x => x.ReducerEtag, x => x.LastReducerSynchronizedEtag, lowestEtag);

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
				return true;
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
		public Etag IndexerEtag { get; set; }

		public Etag LastIndexerSynchronizedEtag { get; set; }

		public Etag ReducerEtag { get; set; }

		public Etag LastReducerSynchronizedEtag { get; set; }
	}
}