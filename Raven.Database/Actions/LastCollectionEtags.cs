// -----------------------------------------------------------------------
//  <copyright file="LastCollectionEtags.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Database.Indexing;

namespace Raven.Database.Actions
{
	public class LastCollectionEtags
	{
		private readonly WorkContext context;
		private ConcurrentDictionary<string, Entry> lastCollectionEtags;

		public LastCollectionEtags(WorkContext context)
		{
			this.context = context;
            this.lastCollectionEtags = new ConcurrentDictionary<string, Entry>(StringComparer.OrdinalIgnoreCase);
		}

		public void InitializeBasedOnIndexingResults()
		{
			var indexDefinitions = context.IndexDefinitionStorage.IndexDefinitions.Values.ToList();

			if (indexDefinitions.Count == 0)
			{
				lastCollectionEtags = new ConcurrentDictionary<string, Entry>(StringComparer.InvariantCultureIgnoreCase);
				return;
			}

			var indexesStats = new List<IndexStats>();

			foreach (var definition in indexDefinitions)
			{
				var indexId = definition.IndexId;

				IndexStats stats = null;
				context.Database.TransactionalStorage.Batch(accessor =>
				{
					var isStale = accessor.Staleness.IsIndexStale(indexId, null, null);
					if (isStale == false)
						stats = accessor.Indexing.GetIndexStats(indexId);
				});

				if (stats == null)
					continue;

				var abstractViewGenerator = context.IndexDefinitionStorage.GetViewGenerator(indexId);
				if (abstractViewGenerator == null)
					continue;

				stats.ForEntityName = abstractViewGenerator.ForEntityNames.ToArray();

				indexesStats.Add(stats);
			}

			var collectionEtags = indexesStats.Where(x => x.ForEntityName.Length > 0)
										.SelectMany(x => x.ForEntityName, (stats, collectionName) => new Tuple<string, Etag>(collectionName, stats.LastIndexedEtag))
										.GroupBy(x => x.Item1, StringComparer.OrdinalIgnoreCase)
										.Select(x => new
										{
											CollectionName = x.Key,
											MaxEtag = x.Min(y => y.Item2)
										})
										.ToDictionary(x => x.CollectionName, y => new Entry { Etag = y.MaxEtag, Updated = SystemTime.UtcNow });

            lastCollectionEtags = new ConcurrentDictionary<string, Entry>(collectionEtags, StringComparer.OrdinalIgnoreCase);
		}

		public bool HasEtagGreaterThan(List<string> collectionsToCheck, Etag etagToCheck)
		{
			var higherEtagExists = true;

			foreach (var collectionName in collectionsToCheck)
			{
                // TODO: Check why lastCollectionEtags == null 
				Entry highestEtagForCollectionEntry;
				if (lastCollectionEtags.TryGetValue(collectionName, out highestEtagForCollectionEntry) && highestEtagForCollectionEntry.Etag != null)
				{
					if (highestEtagForCollectionEntry.Etag.CompareTo(etagToCheck) > 0)
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
			lastCollectionEtags.AddOrUpdate(collectionName, new Entry { Etag = etag, Updated = SystemTime.UtcNow }, (existingEntity, existingEtagEntry) =>
			{
				if (etag.CompareTo(existingEtagEntry.Etag) > 0)
				{
					existingEtagEntry.Etag = etag;
					existingEtagEntry.Updated = SystemTime.UtcNow;
				}

				return existingEtagEntry;
			});
		}

		public void Update(string collectionName)
		{
			lastCollectionEtags.AddOrUpdate(collectionName, new Entry { Etag = null, Updated = SystemTime.UtcNow }, (existingEntity, existingEtagEntry) =>
			{
				existingEtagEntry.Updated = SystemTime.UtcNow;
				return existingEtagEntry;
			});
		}

		public Etag GetLastEtagForCollection(string collectionName)
		{
			Entry entry;
			if (lastCollectionEtags.TryGetValue(collectionName, out entry) && entry.Etag != null)
				return entry.Etag;

			return null;
		}

		public List<string> GetLastChangedCollections(DateTime date)
		{
			return lastCollectionEtags
				.Where(x => x.Value.Updated > date)
				.Select(x => x.Key)
				.ToList();
		}

		private class Entry
		{
			public Etag Etag { get; set; }

			public DateTime Updated { get; set; }
		}
	}
}