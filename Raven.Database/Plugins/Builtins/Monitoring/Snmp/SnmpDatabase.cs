// -----------------------------------------------------------------------
//  <copyright file="SnmpDatabase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;

using Lextm.SharpSnmpLib.Pipeline;

using Raven.Abstractions.Data;
using Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Indexes;
using Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Requests;
using Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database.Statistics;
using Raven.Database.Server.Tenancy;
using Raven.Json.Linq;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp
{
	public class SnmpDatabase
	{
		private readonly ConcurrentDictionary<string, int> loadedIndexes = new ConcurrentDictionary<string, int>(StringComparer.OrdinalIgnoreCase);

		private readonly DatabasesLandlord databaseLandlord;

		private readonly ObjectStore store;

		private readonly string databaseName;

		private readonly int databaseIndex;

		public SnmpDatabase(DatabasesLandlord databaseLandlord, ObjectStore store, string databaseName, int databaseIndex)
		{
			this.databaseLandlord = databaseLandlord;
			this.store = store;
			this.databaseName = databaseName;
			this.databaseIndex = databaseIndex;

			Initialize();
			Update();
		}

		private void Initialize()
		{
			store.Add(new DatabaseName(databaseName, databaseLandlord, databaseIndex));
			store.Add(new DatabaseApproximateTaskCount(databaseName, databaseLandlord, databaseIndex));
			store.Add(new DatabaseCountOfIndexes(databaseName, databaseLandlord, databaseIndex));
			store.Add(new DatabaseCountOfTransformers(databaseName, databaseLandlord, databaseIndex));
			store.Add(new DatabaseStaleIndexes(databaseName, databaseLandlord, databaseIndex));
			store.Add(new DatabaseCountOfAttachments(databaseName, databaseLandlord, databaseIndex));
			store.Add(new DatabaseCountOfDocuments(databaseName, databaseLandlord, databaseIndex));
			store.Add(new DatabaseCurrentNumberOfItemsToIndexInSingleBatch(databaseName, databaseLandlord, databaseIndex));
			store.Add(new DatabaseCurrentNumberOfItemsToReduceInSingleBatch(databaseName, databaseLandlord, databaseIndex));
			store.Add(new DatabaseErrors(databaseName, databaseLandlord, databaseIndex));
			store.Add(new DatabaseId(databaseName, databaseLandlord, databaseIndex));
			store.Add(new DatabaseActiveBundles(databaseName, databaseLandlord, databaseIndex));
			store.Add(new DatabaseLoaded(databaseName, databaseLandlord, databaseIndex));

			store.Add(new DatabaseDocsWritePerSecond(databaseName, databaseLandlord, databaseIndex));
			store.Add(new DatabaseIndexedPerSecond(databaseName, databaseLandlord, databaseIndex));
			store.Add(new DatabaseReducedPerSecond(databaseName, databaseLandlord, databaseIndex));
			store.Add(new DatabaseRequestDurationMean(databaseName, databaseLandlord, databaseIndex));
			store.Add(new DatabaseRequestsPerSecond(databaseName, databaseLandlord, databaseIndex));
		}

		public void Update()
		{
			var database = databaseLandlord.GetDatabaseInternal(databaseName).Result;

			database.Notifications.OnIndexChange += (db, notification) =>
			{
				if (notification.Type != IndexChangeTypes.IndexAdded)
					return;

				loadedIndexes.GetOrAdd(notification.Name, AddIndex);
			};

			AddIndexes(database);
		}

		private void AddIndexes(DocumentDatabase database)
		{
			var indexes = database.IndexStorage.IndexNames;

			foreach (var indexName in indexes)
				loadedIndexes.GetOrAdd(indexName, AddIndex);
		}

		private int AddIndex(string indexName)
		{
			var index = (int)GetOrAddIndexIndex(indexName, databaseLandlord.SystemDatabase);

			store.Add(new DatabaseIndexExists(databaseName, indexName, databaseLandlord, databaseIndex, index));
			store.Add(new DatabaseIndexName(databaseName, indexName, databaseLandlord, databaseIndex, index));
			store.Add(new DatabaseIndexId(databaseName, indexName, databaseLandlord, databaseIndex, index));
			store.Add(new DatabaseIndexAttempts(databaseName, indexName, databaseLandlord, databaseIndex, index));
			store.Add(new DatabaseIndexErrors(databaseName, indexName, databaseLandlord, databaseIndex, index));
			store.Add(new DatabaseIndexPriority(databaseName, indexName, databaseLandlord, databaseIndex, index));
			store.Add(new DatabaseIndexAttempts(databaseName, indexName, databaseLandlord, databaseIndex, index));
			store.Add(new DatabaseIndexSuccesses(databaseName, indexName, databaseLandlord, databaseIndex, index));
			store.Add(new DatabaseIndexReduceAttempts(databaseName, indexName, databaseLandlord, databaseIndex, index));
			store.Add(new DatabaseIndexReduceSuccesses(databaseName, indexName, databaseLandlord, databaseIndex, index));
			store.Add(new DatabaseIndexReduceErrors(databaseName, indexName, databaseLandlord, databaseIndex, index));
			store.Add(new DatabaseIndexTimeSinceLastQuery(databaseName, indexName, databaseLandlord, databaseIndex, index));

			return index;
		}

		private long GetOrAddIndexIndex(string indexName, DocumentDatabase systemDatabase)
		{
			var key = Constants.Monitoring.Snmp.DatabaseIndexMappingDocumentPrefix + databaseName;

			var mappingDocument = systemDatabase.Documents.Get(key, null) ?? new JsonDocument();

			RavenJToken value;
			if (mappingDocument.DataAsJson.TryGetValue(indexName, out value))
				return value.Value<int>();

			var index = 0L;
			systemDatabase.TransactionalStorage.Batch(actions =>
			{
				mappingDocument.DataAsJson[indexName] = index = actions.General.GetNextIdentityValue(key);
				systemDatabase.Documents.Put(key, null, mappingDocument.DataAsJson, mappingDocument.Metadata, null);
			});

			return index;
		}
	}
}