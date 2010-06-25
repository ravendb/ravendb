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
using Raven.Database.Extensions;
using Raven.Database.Json;
using Raven.Database.Linq;
using Raven.Database.Storage;
using Raven.Database.Storage.StorageActions;
using Raven.Database.Tasks;

namespace Raven.Database.Indexing
{
    public class MapReduceIndex : Index
    {
		[CLSCompliant(false)]
		public MapReduceIndex(Directory directory, string name, IndexDefinition indexDefinition)
            : base(directory, name, indexDefinition)
        {
        }

        public override void IndexDocuments(
            AbstractViewGenerator viewGenerator,
            IEnumerable<dynamic> documents,
            WorkContext context,
			IStorageActionsAccessor actions)
        {
            actions.Indexing.SetCurrentIndexStatsTo(name);
        	try
        	{
				var count = 0;
				Func<object, object> documentIdFetcher = null;
				var reduceKeys = new HashSet<string>();
				var documentsWrapped = documents.Select(doc =>
				{
					var documentId = doc.__document_id;
					foreach (var reduceKey in actions.MappedResults.DeleteMappedResultsForDocumentId((string)documentId, name))
					{
						reduceKeys.Add(reduceKey);
					}
					return doc;
				});
				foreach (var doc in RobustEnumeration(documentsWrapped, viewGenerator.MapDefinition, actions, context))
				{
					count++;

					documentIdFetcher = CreateDocumentIdFetcherIfNeeded(documentIdFetcher, doc);

					var docIdValue = documentIdFetcher(doc);
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

					var data = GetMapedData(doc);

					log.DebugFormat("Mapped result for '{0}': '{1}'", name, data);

					var hash = ComputeHash(name, reduceKey);

					actions.MappedResults.PutMappedResult(name, docId, reduceKey, data, hash);

					actions.Indexing.IncrementSuccessIndexing();
				}

				actions.Tasks.AddTask(new ReduceTask
				{
					Index = name,
					ReduceKeys = reduceKeys.ToArray()
				});

				log.DebugFormat("Mapped {0} documents for {1}", count, name);
        	}
        	finally
        	{
        		actions.Indexing.FlushIndexStats();
        	}
        }

        private static JObject GetMapedData(object doc)
        {
        	if (doc is DynamicJsonObject)
                return  ((DynamicJsonObject)doc).Inner;
        	return JObject.FromObject(doc);
        }

    	private static Func<object, object> CreateDocumentIdFetcherIfNeeded(Func<object, object> documentIdFetcher, object doc)
        {
            if (documentIdFetcher != null)
            {
                return documentIdFetcher;
            }
            // document may be DynamicJsonObject if we are using
            // compiled views
            if (doc is DynamicJsonObject)
            {
                documentIdFetcher = i => ((dynamic)i).__document_id;
            }
            else
            {
                var docIdProp = TypeDescriptor.GetProperties(doc).Find("__document_id", false);
                documentIdFetcher = o => docIdProp.GetValue(o);
            }
            return documentIdFetcher;
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

		[CLSCompliant(false)]
		protected override IndexQueryResult RetrieveDocument(Document document, string[] fieldsToFetch)
        {
            if (fieldsToFetch == null || fieldsToFetch.Length == 0)
                fieldsToFetch = document.GetFields().OfType<Fieldable>().Select(x => x.Name()).ToArray();
        	return base.RetrieveDocument(document, fieldsToFetch);
        }

        public override void Remove(string[] keys, WorkContext context)
        {
            context.TransactionaStorage.Batch(actions =>
            {
                var reduceKeys = new HashSet<string>();
                foreach (var key in keys)
                {
                    var reduceKeysFromDocuments = actions.MappedResults.DeleteMappedResultsForDocumentId(key, name);
                    foreach (var reduceKey in reduceKeysFromDocuments)
                    {
                        reduceKeys.Add(reduceKey);
                    }
                }
            	actions.Tasks.AddTask(new ReduceTask
            	{
            		Index = name,
            		ReduceKeys = reduceKeys.ToArray()
            	});

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
									IStorageActionsAccessor actions,
                                    string[] reduceKeys)
        {
            actions.Indexing.SetCurrentIndexStatsTo(name);
            var count = 0;
            Write(indexWriter =>
            {
            	foreach (var reduceKey in reduceKeys)
            	{
					indexWriter.DeleteDocuments(new Term("__reduce_key", reduceKey));
					context.IndexUpdateTriggers.Apply(trigger => trigger.OnIndexEntryDeleted(name, reduceKey));
				}
                PropertyDescriptorCollection properties = null;
                foreach (var doc in RobustEnumeration(mappedResults, viewGenerator.ReduceDefinition, actions, context))
                {
                    count++;
                    var fields = GetFields(doc, ref properties);
                	dynamic reduceKey = viewGenerator.GroupByExtraction(doc);
					if (reduceKey == null)
					{
						throw new InvalidOperationException("Could not find reduce key for " + name + " in the result: " + doc);
					}
					string reduceKeyAsString = ReduceKeyToString(reduceKey);

                	var luceneDoc = new Document();
                    luceneDoc.Add(new Field("__reduce_key", reduceKeyAsString, Field.Store.NO, Field.Index.NOT_ANALYZED));
                    foreach (var field in fields)
                    {
                        luceneDoc.Add(field);
                    }
                    context.IndexUpdateTriggers.Apply(trigger => trigger.OnIndexEntryCreated(name, reduceKeyAsString, luceneDoc));
                    log.DebugFormat("Reduce key {0} result in index {1} gave document: {2}", reduceKeyAsString, name, luceneDoc);
                    indexWriter.AddDocument(luceneDoc);
                    actions.Indexing.IncrementSuccessIndexing();
                }

                return true;
            });
			if(log.IsDebugEnabled)
			{
				log.DebugFormat("Reduce resulted in {0} entries for {1} for reduce keys: {2}", count, name, string.Join(", ", reduceKeys));
			}
        }

        private IEnumerable<AbstractField> GetFields(object doc, ref PropertyDescriptorCollection properties)
        {
            IEnumerable<AbstractField> fields;
            if (doc is DynamicJsonObject)
            {
                fields = AnonymousObjectToLuceneDocumentConverter.Index(((DynamicJsonObject)doc).Inner,
                                                                        indexDefinition, Field.Store.YES);
            }
            else
            {
                properties = properties ?? TypeDescriptor.GetProperties(doc);
                fields = AnonymousObjectToLuceneDocumentConverter.Index(doc, properties, indexDefinition, Field.Store.YES);
            }
            return fields;
        }
    }
}