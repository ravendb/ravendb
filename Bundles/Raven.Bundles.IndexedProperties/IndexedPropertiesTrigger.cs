using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
				//Okay, I understand it more now, inside delete we don't need to do anything, everything happens in OnIndexEntryCreated(..)
				//Map/Reduce will deleted the indexed doc (the results of the Reduce) anytime it needs to re-generate the index
				//The Map steps are still stored in the doc store, to save having to re-calculate those, but the Reduce steps are re-run when needed
				//So anytime the Map/Reduce is updated, we'll be notified of it in the OnIndexEntryCreated(..) trigger.
				Console.WriteLine("DELETING doc {0}:", entryKey);
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

						var changedMade = false;
						foreach (var mapping in _setupDoc.FieldNameMappings)
						{
							Tuple<string, string> localMapping = mapping; //do we really need this??
							NumericField field = numericFields.FirstOrDefault(x => x.Name() == localMapping.Item1 + "_Range");
							if (field != null)
							{
								//This looks a bit wierd, but we want numeric values to be stored in Raven as numbers (not strings)
								resultDoc.DataAsJson[localMapping.Item2] = RavenJToken.Parse(field.GetNumericValue().ToString());
								changedMade = true;
							}
						}
						if (changedMade)
							_database.Put(resultDocId.StringValue(), resultDoc.Etag, resultDoc.DataAsJson, resultDoc.Metadata, null);
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
