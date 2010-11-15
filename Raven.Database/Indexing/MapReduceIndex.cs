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
using Raven.Database.Linq;
using Raven.Database.Storage;
using Raven.Database.Tasks;

namespace Raven.Database.Indexing
{
    public class MapReduceIndex : Index
    {
		
		public MapReduceIndex(Directory directory, string name, IndexDefinition indexDefinition, AbstractViewGenerator viewGenerator)
            : base(directory, name, indexDefinition, viewGenerator)
        {
        }

        public override void IndexDocuments(
			AbstractViewGenerator viewGenerator, 
			IEnumerable<dynamic> documents, 
			WorkContext context, 
			IStorageActionsAccessor actions, 
			DateTime minimumTimestamp)
        {
            actions.Indexing.SetCurrentIndexStatsTo(name);
            var count = 0;
            Func<object, object> documentIdFetcher = null;
            var reduceKeys = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
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
                    logIndexing.DebugFormat("Field {0} is used as the reduce key and cannot be null, skipping document {1}", viewGenerator.GroupByExtraction, docIdValue);
                    continue;
                }
                var reduceKey = ReduceKeyToString(reduceValue);
                var docId = docIdValue.ToString();

                reduceKeys.Add(reduceKey);

                var data = GetMapedData(doc);

                logIndexing.DebugFormat("Mapped result for '{0}': '{1}'", name, data);

                var hash = ComputeHash(name, reduceKey);

                actions.MappedResults.PutMappedResult(name, docId, reduceKey, data, hash);

                actions.Indexing.IncrementSuccessIndexing();
            }

            actions.Tasks.AddTask(new ReduceTask
            {
                Index = name,
                ReduceKeys = reduceKeys.ToArray()
            }, minimumTimestamp);

            logIndexing.DebugFormat("Mapped {0} documents for {1}", count, name);
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
            	}, DateTime.UtcNow);

            });
            Write(context, writer =>
            {
				if (logIndexing.IsDebugEnabled)
                {
					logIndexing.DebugFormat("Deleting ({0}) from {1}", string.Format(", ", keys), name);
                }
                writer.DeleteDocuments(keys.Select(k => new Term("__reduce_key", k.ToLowerInvariant())).ToArray());
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
            Write(context, indexWriter =>
            {
                var batchers = context.IndexUpdateTriggers.Select(x=>x.CreateBatcher(name))
                    .Where(x=>x!=null)
                    .ToList();
                foreach (var reduceKey in reduceKeys)
            	{
            	    var entryKey = reduceKey;
            	    indexWriter.DeleteDocuments(new Term("__reduce_key", entryKey.ToLowerInvariant()));
                    batchers.ApplyAndIgnoreAllErrors(
                        exception =>
                        {
                            logIndexing.WarnFormat(exception,
                                                   "Error when executed OnIndexEntryDeleted trigger for index '{0}', key: '{1}'",
                                                   name, entryKey);
                            context.AddError(name,
                                           entryKey,
                                           exception.Message
                              );
                        },
                        trigger => trigger.OnIndexEntryDeleted(name, entryKey));
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
                    luceneDoc.Add(new Field("__reduce_key", reduceKeyAsString.ToLowerInvariant(), Field.Store.NO, Field.Index.NOT_ANALYZED));
                    foreach (var field in fields)
                    {
                        luceneDoc.Add(field);
                    }
                    batchers.ApplyAndIgnoreAllErrors(
                        exception =>
                        {
                            logIndexing.WarnFormat(exception,
                                                   "Error when executed OnIndexEntryCreated trigger for index '{0}', key: '{1}'",
                                                   name, reduceKeyAsString);
                            context.AddError(name,
                                           reduceKeyAsString,
                                           exception.Message
                              );
                        },
                        trigger => trigger.OnIndexEntryCreated(name, reduceKeyAsString, luceneDoc));
					logIndexing.DebugFormat("Reduce key {0} result in index {1} gave document: {2}", reduceKeyAsString, name, luceneDoc);
                    indexWriter.AddDocument(luceneDoc);
                    actions.Indexing.IncrementSuccessIndexing();
                }
                batchers.ApplyAndIgnoreAllErrors(
                    e =>
                    {
                        logIndexing.Warn("Failed to dispose on index update trigger", e);
                        context.AddError(name, null, e.Message);
                    },
                    x => x.Dispose());
                return true;
            });
			if (logIndexing.IsDebugEnabled)
			{
				logIndexing.DebugFormat("Reduce resulted in {0} entries for {1} for reduce keys: {2}", count, name, string.Join(", ", reduceKeys));
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
