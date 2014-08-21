// -----------------------------------------------------------------------
//  <copyright file="LastCollectionEtags.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Database.Indexing;

namespace Raven.Database.Actions
{
	public class LastCollectionEtags
	{
		private readonly WorkContext context;
		private ConcurrentDictionary<string, Etag> lastCollectionEtags;

		public LastCollectionEtags(WorkContext context)
		{
			this.context = context;
		}

		public void InitializeBasedOnIndexingResults()
		{
			List<IndexStats> indexesStats = null;
			context.Database.TransactionalStorage.Batch(accessor =>
			{
				indexesStats = accessor.Indexing.GetIndexesStats().ToList();
			});

			if (indexesStats.Count == 0)
			{
				lastCollectionEtags = new ConcurrentDictionary<string, Etag>();
				return;
			}

			foreach (var stat in indexesStats)
			{
				stat.ForEntityName = context.IndexDefinitionStorage.GetViewGenerator(stat.Id).ForEntityNames.ToList();
			}

			var collectionEtags = indexesStats.Where(x => x.ForEntityName.Count > 0)
										.SelectMany(x => x.ForEntityName, (stats, collectionName) => new Tuple<string, Etag>(collectionName, stats.LastIndexedEtag))
										.GroupBy(x => x.Item1)
										.Select(x => new
										{
											CollectionName = x.Key, MaxEtag = x.Max(y => y.Item2)
										})
										.ToDictionary(x => x.CollectionName, y => y.MaxEtag);

			lastCollectionEtags = new ConcurrentDictionary<string, Etag>(collectionEtags);
		}

		public bool HasEtagGreaterThan(List<string> collectionsToCheck, Etag etagToCheck)
		{
			var higherEtagExists = true;

			foreach (var collectionName in collectionsToCheck)
			{
				Etag highestEtagForCollection;
				if (lastCollectionEtags.TryGetValue(collectionName, out highestEtagForCollection))
				{
					if (highestEtagForCollection.CompareTo(etagToCheck) > 0)
						return true;

					higherEtagExists = false;
				}
				else
				{
					return true;
				}
			}

			return higherEtagExists;
		}

		public void Update(string collectionName, Etag etag)
		{
			lastCollectionEtags.AddOrUpdate(collectionName, etag, (existingEntity, existingEtag) => etag.CompareTo(existingEtag) > 0 ? etag : existingEtag);
		}

		public Etag GetLastEtagForCollection(string collectionName)
		{
			Etag result;
			if (lastCollectionEtags.TryGetValue(collectionName, out result))
				return result;

			return null;
		}
	}
}