using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Json;
using Raven.Database.Linq;
using Raven.Database.Storage;
using Raven.Database.Storage.StorageActions;
using Raven.Database.Tasks;

namespace Raven.Database.Indexing
{
	public class MapReduceIndex : Index
	{
		public MapReduceIndex(Directory directory, string name, IndexDefinition indexDefinition)
			: base(directory, name, indexDefinition)
		{
		}

		public override void IndexDocuments(
			AbstractViewGenerator viewGenerator,
			IEnumerable<object> documents,
			WorkContext context,
			DocumentStorageActions actions)
		{
			actions.SetCurrentIndexStatsTo(name);
			var count = 0;
			PropertyDescriptor documentIdPropertyDescriptor = null;
			var reduceKeys = new HashSet<string>();
			foreach (var doc in RobustEnumeration(documents, viewGenerator.MapDefinition, actions, context))
			{
				count++;

				if (documentIdPropertyDescriptor == null)
				{
					var props = TypeDescriptor.GetProperties(doc);
					documentIdPropertyDescriptor = props.Find("__document_id", false);
				}

				var docIdValue = documentIdPropertyDescriptor.GetValue(doc);
				if (docIdValue == null)
					throw new InvalidOperationException("Could not find document id for this document");

				var reduceValue = viewGenerator.GroupByExtraction(doc);
				if (reduceValue == null)
				{
					log.DebugFormat("Field {0} is used as the reduce key and cannot be null, skipping document {1}", viewGenerator.GroupByExtraction, docIdValue);
					continue;
				}
				var reduceKey = ReduceKeyToString(reduceValue);
				var docId = docIdValue.ToString();

				reduceKeys.Add(reduceKey);

				var data = JObject.FromObject(doc).ToString(Formatting.None, new JsonEnumConverter(), new JsonLuceneNumberConverter());
				
				log.DebugFormat("Mapped result for '{0}': '{1}'", name, data);

			    var hash = ComputeHash(name, reduceKey);

			    actions.PutMappedResult(name, docId, reduceKey, data, hash);

				actions.IncrementSuccessIndexing();
			}

			foreach (var reduceKey in reduceKeys)
			{
				actions.AddTask(new ReduceTask
				{
					Index = name,
					ReduceKey = reduceKey
				});
			}

			log.DebugFormat("Mapped {0} documents for {1}", count, name);
		}

	    public static byte[] ComputeHash(string name, string reduceKey)
	    {
            using (var sha256 = SHA256.Create())
                return sha256.ComputeHash(Encoding.UTF8.GetBytes(name + "/" + reduceKey));
	    }

	    private static string ReduceKeyToString(object reduceValue)
		{
			if (reduceValue is string || reduceValue is ValueType)
				return reduceValue.ToString();
			return JToken.FromObject(reduceValue).ToString(Formatting.None);
		}

		protected override IndexQueryResult RetrieveDocument(Document document, string[] fieldsToFetch)
		{
			if (fieldsToFetch == null || fieldsToFetch.Length == 0)
				fieldsToFetch = document.GetFields().OfType<Fieldable>().Select(x => x.Name()).ToArray();
			return new IndexQueryResult
			{
				Key = null,
				Projection =
					new JObject(
					fieldsToFetch.Concat(new[] {"__document_id"}).Distinct()
						.SelectMany(name => document.GetFields(name) ?? new Field[0])
						.Where(x => x != null)
						.Select(fld => new JProperty(fld.Name(), fld.StringValue()))
						.GroupBy(x => x.Name)
						.Select(g =>
						{
							if (g.Count() == 1)
								return g.First();
							return new JProperty(g.Key,
							                     g.Select(x => x.Value)
								);
						})
					)
			};
		}

		public override void Remove(string[] keys, WorkContext context)
		{
			context.TransactionaStorage.Batch(actions =>
			{
				var reduceKeys = new HashSet<string>();
				foreach (var key in keys)
				{
					var reduceKeysFromDocuments = actions.DeleteMappedResultsForDocumentId(key, name);
					foreach (var reduceKey in reduceKeysFromDocuments)
					{
						reduceKeys.Add(reduceKey);
					}
				}

				foreach (var reduceKey in reduceKeys)
				{
					actions.AddTask(new ReduceTask
					{
						Index = name,
						ReduceKey = reduceKey,
					});
				}

			});
			Write(writer =>
			{
				if (log.IsDebugEnabled)
				{
					log.DebugFormat("Deleting ({0}) from {1}", string.Format(", ", keys), name);
				}
				writer.DeleteDocuments(keys.Select(k => new Term("__document_id", k)).ToArray());
				return true;
			});
		}

		public void ReduceDocuments(AbstractViewGenerator viewGenerator,
		                            IEnumerable<object> mappedResults,
		                            WorkContext context,
		                            DocumentStorageActions actions,
		                            string reduceKey)
		{
			actions.SetCurrentIndexStatsTo(name);
			var count = 0;
			Write(indexWriter =>
			{
				indexWriter.DeleteDocuments(new Term("__reduce_key", reduceKey));
				var converter = new AnonymousObjectToLuceneDocumentConverter();
				PropertyDescriptorCollection properties = null;
				foreach (var doc in RobustEnumeration(mappedResults, viewGenerator.ReduceDefinition, actions, context))
				{
					count++;
					if (properties == null)
					{
						properties = TypeDescriptor.GetProperties(doc);
					}
					var fields = converter.Index(doc, properties, indexDefinition, Field.Store.YES);

					var luceneDoc = new Document();
					luceneDoc.Add(new Field("__reduce_key", reduceKey, Field.Store.NO, Field.Index.NOT_ANALYZED));
					foreach (var field in fields)
					{
						luceneDoc.Add(field);
					}
					log.DebugFormat("Reduce key {0} result in index {1} gave document: {2}", reduceKey, name, luceneDoc);
					indexWriter.AddDocument(luceneDoc);
					actions.IncrementSuccessIndexing();
				}

				return true;
			});
			log.DebugFormat("Reduce resulted in {0} entries for {1} for reduce key {2}", count, name, reduceKey);
		}
	}
}