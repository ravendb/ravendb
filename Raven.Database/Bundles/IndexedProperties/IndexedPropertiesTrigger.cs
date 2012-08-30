using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using Lucene.Net.Documents;
using NLog;
using Raven.Abstractions.Data;
using Raven.Database;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Raven.Json.Linq;
using Document = Lucene.Net.Documents.Document;
using Raven.Abstractions.Extensions;

namespace Raven.Bundles.IndexedProperties
{
	[InheritedExport]
	[ExportMetadata("Bundle", "IndexedProperties")]
	public class IndexedPropertiesTrigger : AbstractIndexUpdateTrigger
	{
		private static readonly Logger log = LogManager.GetCurrentClassLogger();

		public override AbstractIndexUpdateTriggerBatcher CreateBatcher(string indexName)
		{
			//Only apply the trigger if there is a setup doc for this particular index
			var jsonSetupDoc = Database.Get("Raven/IndexedProperties/" + indexName, null);
			if (jsonSetupDoc == null)
				return null;
			var abstractViewGenerator = Database.IndexDefinitionStorage.GetViewGenerator(indexName);
			var setupDoc = jsonSetupDoc.DataAsJson.JsonDeserialization<IndexedPropertiesSetupDoc>();
			return new IndexPropertyBatcher(Database, setupDoc, indexName, abstractViewGenerator);
		}

		public class IndexPropertyBatcher : AbstractIndexUpdateTriggerBatcher
		{
			private readonly DocumentDatabase database;
			private readonly IndexedPropertiesSetupDoc setupDoc;
			private readonly string index;
			private readonly AbstractViewGenerator viewGenerator;
			private readonly HashSet<string> itemsToRemove = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);

			public IndexPropertyBatcher(DocumentDatabase database, IndexedPropertiesSetupDoc setupDoc, string index, AbstractViewGenerator viewGenerator)
			{
				this.database = database;
				this.setupDoc = setupDoc;
				this.index = index;
				this.viewGenerator = viewGenerator;
			}

			public override void OnIndexEntryDeleted(string entryKey)
			{
				//Want to handle this scenario:
				// - Customer/1 has 2 orders (order/3 & order/5)
				// - Map/Reduce runs and AvgOrderCost in "customer/1" is set to the average cost of "order/3" and "order/5" (8.56 for example)
				// - "order/3" and "order/5" are deleted (so customer/1 will no longer be included in the results of the Map/Reduce
				// - I think we need to write back to the "customer/1" doc and delete the AvgOrderCost field in the Json (otherwise it'll still have the last value of 8.56)

				RavenJObject entry;
				try
				{
					entry = RavenJObject.Parse(entryKey);
				}
				catch (Exception e)
				{
					log.WarnException("Could not properly parse entry key for index: " + index,e);
					return;

				}
				var documentId = entry.Value<string>(setupDoc.DocumentKey);
				if(documentId == null)
				{
					log.Warn("Could not find document id property '{0}' in '{1}' for index '{2}'", setupDoc.DocumentKey, entryKey, index);
					return;
				}

				itemsToRemove.Add(documentId);

			}

			public override void OnIndexEntryCreated(string entryKey, Document document)
			{
				var resultDocId = document.GetField(setupDoc.DocumentKey);
				if (resultDocId == null)
				{
					log.Warn("Could not find document id property '{0}' in '{1}' for index '{2}'", setupDoc.DocumentKey, entryKey, index);
					return;
				}

				var documentId = resultDocId.StringValue;

				itemsToRemove.Remove(documentId);

				var resultDoc = database.Get(documentId, null);
				if (resultDoc == null)
				{
					log.Warn("Could not find a document with the id '{0}' for index '{1}'", documentId, index);
					return;
				}

				var entityName = resultDoc.Metadata.Value<string>(Constants.RavenEntityName);
				if(entityName != null && viewGenerator.ForEntityNames.Contains(entityName))
				{
					log.Warn(
						"Rejected update for a potentially recursive update on document '{0}' because the index '{1}' includes documents with entity name of '{2}'",
						documentId, index, entityName);
					return;
				}
				if(viewGenerator.ForEntityNames.Count == 0)
				{
					log.Warn(
						"Rejected update for a potentially recursive update on document '{0}' because the index '{1}' includes all documents",
						documentId, index);
					return;
				}

				var changesMade = false;
				foreach (var mapping in setupDoc.FieldNameMappings)
				{
					var field = 
						document.GetFieldable(mapping.Key + "_Range") ??
						document.GetFieldable(mapping.Key);
					if (field == null)
						continue;
					var numericField = field as NumericField;
					if (numericField != null)
					{
						resultDoc.DataAsJson[mapping.Value] = new RavenJValue(numericField.NumericValue);
					}
					else
					{
						resultDoc.DataAsJson[mapping.Value] = field.StringValue;
					}
					changesMade = true;
				}
				if (changesMade)
					database.Put(documentId, resultDoc.Etag, resultDoc.DataAsJson, resultDoc.Metadata, null);
			}

			public override void Dispose()
			{
				foreach (var documentId in itemsToRemove)
				{
					var resultDoc = database.Get(documentId, null);
					if (resultDoc == null)
					{
						log.Warn("Could not find a document with the id '{0}' for index '{1}", documentId, index);
						return;
					}
					var changesMade = false;
					foreach (var mapping in from mapping in setupDoc.FieldNameMappings
											where resultDoc.DataAsJson.ContainsKey(mapping.Value)
											select mapping)
					{
						resultDoc.DataAsJson.Remove(mapping.Value);
						changesMade = true;
					}
					if (changesMade)
						database.Put(documentId, resultDoc.Etag, resultDoc.DataAsJson, resultDoc.Metadata, null);
		
				}

				base.Dispose();
			}
		}
	}
}
