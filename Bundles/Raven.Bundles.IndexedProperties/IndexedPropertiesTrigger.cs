using System;
using System.Linq;
using Lucene.Net.Documents;
using Raven.Abstractions.Data;
using Raven.Client.IndexedProperties;
using Raven.Database;
using Raven.Database.Plugins;
using Raven.Json.Linq;
using Document = Lucene.Net.Documents.Document;
using Raven.Abstractions.Extensions;

namespace Raven.Bundles.IndexedProperties
{
	public class IndexedPropertiesTrigger : AbstractIndexUpdateTrigger
	{
		public override AbstractIndexUpdateTriggerBatcher CreateBatcher(string indexName)
		{
			//Only apply the trigger if there is a setup doc for this particular index
			var jsonSetupDoc = Database.Get("Raven/IndexedProperties/" + indexName, null);
			if (jsonSetupDoc != null)
			{
				var setupDoc = jsonSetupDoc.DataAsJson.JsonDeserialization<SetupDoc>();
				return new IndexPropertyBatcher(Database, setupDoc);
			}
			return null;
		}

		public class IndexPropertyBatcher : AbstractIndexUpdateTriggerBatcher
		{
			private readonly DocumentDatabase _database;
			private readonly SetupDoc _setupDoc;

			public IndexPropertyBatcher(DocumentDatabase database, SetupDoc setupDoc)
			{
				_database = database;
				_setupDoc = setupDoc;
			}

			public override void OnIndexEntryDeleted(string entryKey)
			{
				//Want to handle this scenario:
				// - Customer/1 has 2 orders (order/3 & order/5)
				// - Map/Reduce runs and AvgOrderCost in "customer/1" is set to the average cost of "order/3" and "order/5" (8.56 for example)
				// - "order/3" and "order/5" are deleted (so customer/1 will no longer be included in the results of the Map/Reduce
				// - I think we need to write back to the "customer/1" doc and delete the AvgOrderCost field in the Json (otherwise it'll still have the last value of 8.56)

				var parts = entryKey.Split(new[] {':', '{', '}', ' ', '"'}, StringSplitOptions.RemoveEmptyEntries);
				if (parts.Length != 2)
					return;

				var resultDoc = _database.Get(parts[1], null);
				if (resultDoc == null)
				{
					//Log an error?
					return;
				}
				var changesMade = false;
				foreach (var mapping in _setupDoc.FieldNameMappings)
				{
					var jsonData = resultDoc.DataAsJson;
					if (jsonData.ContainsKey(mapping.Item2))
					{
						resultDoc.DataAsJson.Remove(mapping.Item2);
						changesMade = true;
					}
				}
				if (changesMade)
					_database.Put(parts[1], resultDoc.Etag, resultDoc.DataAsJson, resultDoc.Metadata, null);
			}

			public override void OnIndexEntryCreated(string entryKey, Document document)
			{
				try
				{
					var fields = document.GetFields().OfType<Field>().ToList();
					var numericFields = document.GetFields().OfType<NumericField>().ToList();
					var isMapReduce = fields.Any(x => x.Name() == Constants.ReduceKeyFieldName);
					if (isMapReduce) //Do we need this, it's the responsiblity of the user to make sure the SetupDoc has the right index?
					{
						var resultDocId = fields.FirstOrDefault(x => x.Name() == _setupDoc.DocumentKey);
						if (resultDocId == null)
						{
							//Log an error?
							return;
						}
						var resultDoc = _database.Get(resultDocId.StringValue(), null);
						if (resultDoc == null)
						{
							//Log an error?
							return;
						}

						var changesMade = false;
						foreach (var mapping in _setupDoc.FieldNameMappings)
						{
							Tuple<string, string> localMapping = mapping; //do we really need this (to stop "access to modified closure issue")??
							NumericField field = numericFields.FirstOrDefault(x => x.Name() == localMapping.Item1 + "_Range");
							if (field != null)
							{
								//This looks a bit wierd, but we want numeric values to be stored in Raven as numbers (not strings)
								resultDoc.DataAsJson[localMapping.Item2] = RavenJToken.Parse(field.GetNumericValue().ToString());
								changesMade = true;
							}
						}
						if (changesMade)
							_database.Put(resultDocId.StringValue(), resultDoc.Etag, resultDoc.DataAsJson, resultDoc.Metadata, null);
					}
				}
				catch (Exception) // we don't errors in the trigger to propogate out
				{
					//Log an error?
				}
			}
		}
	}
}
