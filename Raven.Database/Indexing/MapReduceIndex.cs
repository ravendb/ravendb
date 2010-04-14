using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Linq;
using Raven.Database.Storage;
using Raven.Database.Tasks;

namespace Raven.Database.Indexing
{
	public class MapReduceIndex : Index
	{
		public MapReduceIndex(Directory directory, string name) : base(directory, name)
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
			PropertyDescriptor groupByPropertyDescriptor = null;
			PropertyDescriptor documentIdPropertyDescriptor = null;
			var reduceKeys = new HashSet<string>();
			foreach (var doc in RobustEnumeration(documents, viewGenerator.MapDefinition, actions, context))
			{
				count++;

				if (groupByPropertyDescriptor == null)
				{
					var props = TypeDescriptor.GetProperties(doc);
					groupByPropertyDescriptor = props.Find(viewGenerator.GroupByField, false);
					documentIdPropertyDescriptor = props.Find("__document_id", false);
				}

				var docIdValue = documentIdPropertyDescriptor.GetValue(doc);
				if (docIdValue == null)
					throw new InvalidOperationException("Could not find document id for this document");
				
				var reduceValue = groupByPropertyDescriptor.GetValue(doc);
				if (reduceValue == null)
				{
					log.DebugFormat("Field {0} is used as the reduce key and cannot be null, skipping document {1}", viewGenerator.GroupByField, docIdValue);
					continue;
				}
				var reduceKey = reduceValue.ToString();
				var docId = docIdValue.ToString();

				reduceKeys.Add(reduceKey);

				actions.PutMappedResult(name, docId, reduceKey, JObject.FromObject(doc).ToString(Formatting.None));

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

				actions.Commit();
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
				indexWriter.DeleteDocuments(new Term(viewGenerator.GroupByField, reduceKey));
				var converter = new AnonymousObjectToLuceneDocumentConverter();
				PropertyDescriptorCollection properties = null;
				foreach (var doc in RobustEnumeration(mappedResults, viewGenerator.ReduceDefinition, actions, context))
				{
					count++;
					if (properties == null)
					{
						properties = TypeDescriptor.GetProperties(doc);
					}
					var fields = converter.Index(doc, properties);

					var luceneDoc = new Document();
					foreach (var field in fields)
					{
						luceneDoc.Add(field);
					}

					indexWriter.AddDocument(luceneDoc);
					actions.IncrementSuccessIndexing();
				}

				return true;
			});
			log.DebugFormat("Reduce resulted in {0} entires for {1} for reduce key {2}", count, name, reduceKey);
		}
	}
}