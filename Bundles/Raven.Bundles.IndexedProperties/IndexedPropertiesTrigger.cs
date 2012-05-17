using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Lucene.Net.Documents;
using Raven.Abstractions.Data;
using Raven.Database;
using Raven.Database.Plugins;
using Raven.Json.Linq;
using Document = Lucene.Net.Documents.Document;

namespace Raven.Bundles.IndexedProperties
{
	public class IndexedPropertiesTrigger : AbstractIndexUpdateTrigger
	{
		//We can't keep any state in the IndexPropertyBatcher itself, as it's create each time a batch is run
		//Whereas this class (IndexPropertyTrigger is only create once during the app lifetime (as far as I can tell)
		private readonly ConcurrentDictionary<string, Guid?> previouslyModifiedDocIds = new ConcurrentDictionary<string, Guid?>();

		public override AbstractIndexUpdateTriggerBatcher CreateBatcher(string indexName)
		{
			//Only apply the trigger if there is a setup doc for this particular index
			var setupDoc = Database.Get("Raven/IndexedProperties/" + indexName, null);
			if (setupDoc != null)
				return new IndexPropertyBatcher(this, Database, setupDoc);
			return null;
		}

		public class IndexPropertyBatcher : AbstractIndexUpdateTriggerBatcher
		{
			private readonly IndexedPropertiesTrigger _parent;
			private readonly DocumentDatabase _database;
			private readonly JsonDocument _setupDoc;

			public IndexPropertyBatcher(IndexedPropertiesTrigger parent, DocumentDatabase database, JsonDocument setupDoc)
			{
				_parent = parent;
				_database = database;
				_setupDoc = setupDoc;
			}

			public override void OnIndexEntryDeleted(string entryKey)
			{
				//Okay, I understand it more now, inside delete we don't need to do anything, everything happens in OnIndexEntryCreated(..)
				//Map/Reduce will deleted the indexed doc (the results of the Reduce) anytime it needs to re-generate the index
				//The Map steps are still stored in the doc store, to save having to re-calculate those, but the Reduce steps are re-run when needed
				//So anytime the Map/Reduce is updated, we'll here about it in the OnIndexEntryCreated(..) trigger.
				Console.WriteLine("DELETING doc {0}:", entryKey);

				//Probably all we need to do here is removed the calculated value from the doc or set it to zero??
				//Is there ever a scenario where it's deleted but no re-created below??
			}

			public override void OnIndexEntryCreated(string entryKey, Document document)
			{
				Console.WriteLine("INDEXING doc {0}:", entryKey);
				//PrintIndexDocInfo(document);

				try
				{
					var fields = document.GetFields().OfType<Field>().ToList();
					var numericFields = document.GetFields().OfType<NumericField>().ToList();
					var isMapReduce = fields.Any(x => x.Name() == Constants.ReduceKeyFieldName);
					if (isMapReduce)
					{
						//All these magic strings will eventually be read from a configuration doc that the user will have created
						var docKeyField = fields.FirstOrDefault(x => x.Name() == "CustomerId");
						var totalCostField = fields.FirstOrDefault(x => x.Name() == "TotalCost");
						//var countField = fields.FirstOrDefault(x => x.Name() == "Count");
						NumericField countField = numericFields.FirstOrDefault(x => x.Name() == "Count_Range");
						//Field avgField = numericFields.FirstOrDefault(x => x.Name() == "Average");
						NumericField avgField = numericFields.FirstOrDefault(x => x.Name() == "Average_Range");
						if (docKeyField != null && totalCostField != null && countField != null)
						{
							var docId = docKeyField.StringValue();
							//Does this robustly stop us from handling a trigger that we ourselved caused (by modifying a doc)????
							//Guid? entry;
							//if (_parent.previouslyModifiedDocIds.TryGetValue(docId, out entry))
							//{
							//    Guid? currentEtag = _database.GetDocumentMetadata(docId, null).Etag;
							//    if (entry != null && currentEtag == entry.Value)
							//    {
							//        Guid? removedValue;
							//        var removed = _parent.previouslyModifiedDocIds.TryRemove(docId, out removedValue);
							//        //Not sure we need to do anything if TryRemove failed, it just means that the key was removed in another thread
							//        return;
							//    }
							//}
							var existingDoc = _database.Get(docId, null);
							var avgNum = (double)avgField.GetNumericValue();
							//Console.WriteLine("DocId = {0}, TotalCost = {1}, Count = {2}, Avg = {3:0.0000}",
							//                  docId, totalCostField.StringValue(), countField.StringValue(), avgNum);
							//Need a better way of doing this, should use NumericField instead of Field
							var actualType = avgField.GetNumericValue();
							Type fieldType = actualType.GetType();
							//This looks a bit wierd, but we want numeric values to be stored in Raven as numbers (not strings)
							existingDoc.DataAsJson["AverageOrderCost"] = RavenJToken.Parse(avgField.GetNumericValue().ToString());
							existingDoc.DataAsJson["NumberOfOrders"] = RavenJToken.Parse(countField.GetNumericValue().ToString());

							//TODO the problem with doing this is that it causes the whole Map/Reduce index to be rebuilt!!!!!
							//So we can ignore the 2nd trigger okay, but this is costly and might cause some staleness issues?!!?
							_database.Put(docId, existingDoc.Etag, existingDoc.DataAsJson, existingDoc.Metadata, null);
							//Guid? etag = _database.GetDocumentMetadata(docId, null).Etag;
							//_parent.previouslyModifiedDocIds.AddOrUpdate(docId, etag, (key, value) => etag);
						}
						else
						{
							Console.WriteLine("\n*** The indexed doc doesn't have the expected fields ***\n");
						}
					}
				}
				catch (Exception ex)
				{
					Console.WriteLine(ex.Message);
				}
			}

			private void PrintIndexDocInfo(Document document)
			{
				foreach (object field in document.GetFields())
				{
					try
					{
						if (field is NumericField)
						{
							var numbericField = field as NumericField;
							Console.WriteLine("\t{0}: {1} - (NumericField)", numbericField.Name(), numbericField.GetNumericValue());
						}
						else if (field is Field)
						{
							var stdField = field as Field;
							Console.WriteLine("\t{0}: {1} - (Field)", stdField.Name(), stdField.StringValue());
						}
						else
						{
							Console.WriteLine("Unknown field type: " + field.GetType());
						}
					}
					catch (Exception ex)
					{
						Console.WriteLine(ex.Message);
					}
				}
			}
		}
	}
}
