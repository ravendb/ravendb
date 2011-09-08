//-----------------------------------------------------------------------
// <copyright file="MapReduceIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Newtonsoft.Json;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Linq;
using Raven.Database.Storage;
using Raven.Database.Tasks;
using Raven.Json.Linq;

namespace Raven.Database.Indexing
{
	public class MapReduceIndex : Index
	{
		public MapReduceIndex(Directory directory, string name, IndexDefinition indexDefinition, AbstractViewGenerator viewGenerator)
			: base(directory, name, indexDefinition, viewGenerator)
		{
		}

		public override bool IsMapReduce
		{
			get { return true; }
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
			
			// we mark the reduce keys to delete when we delete the mapped results, then we remove
			// any reduce key that is actually being used to generate new mapped results
			// this way, only reduces that removed data will force us to use the tasks approach
			var reduceKeysToDelete = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
			var documentsWrapped = documents.Select(doc =>
			{
				var documentId = doc.__document_id;
				foreach (var reduceKey in actions.MappedResults.DeleteMappedResultsForDocumentId((string)documentId, name))
				{
					reduceKeysToDelete.Add(reduceKey);
				}
				return doc;
			});
			foreach (var mappedResultFromDocument in GroupByDocumentId(RobustEnumerationIndex(documentsWrapped, viewGenerator.MapDefinitions, actions, context)))
			{
				foreach (var doc in RobustEnumerationReduceDuringMapPhase(mappedResultFromDocument, viewGenerator.ReduceDefinition, actions, context))
				{
					count++;

					var reduceValue = viewGenerator.GroupByExtraction(doc);
					if (reduceValue == null)
					{
						logIndexing.Debug("Field {0} is used as the reduce key and cannot be null, skipping document {1}", viewGenerator.GroupByExtraction, mappedResultFromDocument.Key);
						continue;
					}
					var reduceKey = ReduceKeyToString(reduceValue);
					var docId = mappedResultFromDocument.Key.ToString();

					reduceKeysToDelete.Remove((string)reduceKey);

					var data = GetMapedData(doc);

					logIndexing.Debug("Mapped result for '{0}': '{1}'", name, data);

					var hash = ComputeHash(name, reduceKey);

					actions.MappedResults.PutMappedResult(name, docId, reduceKey, data, hash);

					actions.Indexing.IncrementSuccessIndexing();
				}
			}

			if (reduceKeysToDelete.Count > 0)
			{
				actions.Tasks.AddTask(new ReduceTask
				{
					Index = name,
					ReduceKeys = reduceKeysToDelete.ToArray()
				}, minimumTimestamp);
			}

			logIndexing.Debug("Mapped {0} documents for {1}", count, name);
		}

		// we don't use the usual GroupBy, because that isn't streaming
		// we rely on the fact that all values from the same docs are always outputed at 
		// the same time, so we can take advantage of this fact
		private static IEnumerable<IGrouping<object ,dynamic>> GroupByDocumentId(IEnumerable<object> docs)
		{
			var enumerator = docs.GetEnumerator();
			if(enumerator.MoveNext()==false)
				yield break;

			while (true)
			{
				var groupByDocumentId = new Grouping(GetDocumentId(enumerator.Current), enumerator);
				yield return groupByDocumentId;
				if (groupByDocumentId.Done)
					break;
			}
		}

		private class Grouping : IGrouping<object, object>
		{
			private readonly IEnumerator enumerator;
			private bool newKeyFound;
			public bool Done { get; private set; }
			public IEnumerator<object> GetEnumerator()
			{
				if(newKeyFound || Done)
					yield break;
				yield return enumerator.Current;

				if (enumerator.MoveNext() == false)
					Done = true;

				var documentId = GetDocumentId(enumerator.Current);

				if (Equals(documentId, Key) == false)
					newKeyFound = true;
			}

			IEnumerator IEnumerable.GetEnumerator()
			{
				return GetEnumerator();
			}

			public object Key { get; private set; }

			public Grouping(object key, IEnumerator enumerator)
			{
				this.enumerator = enumerator;
				Key = key;
			}
		}

		private static RavenJObject GetMapedData(object doc)
		{
			if (doc is DynamicJsonObject)
				return ((DynamicJsonObject)doc).Inner;
			return RavenJObject.FromObject(doc, JsonExtensions.CreateDefaultJsonSerializer());
		}

		private static readonly ConcurrentDictionary<Type, Func<object, object>> documentIdFetcherCache = new ConcurrentDictionary<Type, Func<object, object>>();
		private static object GetDocumentId(object doc)
		{
			return documentIdFetcherCache.GetOrAdd(doc.GetType(), type =>
			{
				// document may be DynamicJsonObject if we are using compiled views
				if (typeof (DynamicJsonObject) == type)
				{
					return i => ((dynamic) i).__document_id;
				}
				var docIdProp = TypeDescriptor.GetProperties(doc).Find(Abstractions.Data.Constants.DocumentIdFieldName, false);
				return docIdProp.GetValue;
			})(doc);
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
			var dynamicJsonObject = reduceValue as DynamicJsonObject;
			if (dynamicJsonObject != null)
				return dynamicJsonObject.Inner.ToString(Formatting.None);
			return RavenJToken.FromObject(reduceValue).ToString(Formatting.None);
		}


		protected override IndexQueryResult RetrieveDocument(Document document, FieldsToFetch fieldsToFetch, float score)
		{
			if (fieldsToFetch.IsProjection == false)
				fieldsToFetch = fieldsToFetch.CloneWith(document.GetFields().OfType<Fieldable>().Select(x => x.Name()).ToArray());
			fieldsToFetch.EnsureHasField(Abstractions.Data.Constants.ReduceKeyFieldName);
			return base.RetrieveDocument(document, fieldsToFetch, score);
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
				}, SystemTime.UtcNow);

			});
			Write(context, (writer, analyzer) =>
			{
				logIndexing.Debug(() => string.Format("Deleting ({0}) from {1}", string.Join(", ", keys), name));
				writer.DeleteDocuments(
					keys.Select(k => new Term(Abstractions.Data.Constants.ReduceKeyFieldName, k.ToLowerInvariant())).ToArray());
				return true;
			});
		}


		// This method may be called concurrently, by both the ReduceTask (for removal)
		// and by the ReducingExecuter (for add/modify). This is okay with us, since the 
		// Write() call is already handling locking properly
		public void ReduceDocuments(AbstractViewGenerator viewGenerator,
									IEnumerable<object> mappedResults,
									WorkContext context,
									IStorageActionsAccessor actions,
									string[] reduceKeys)
		{
			var count = 0;
			Write(context, (indexWriter, analyzer) =>
			{
				actions.Indexing.SetCurrentIndexStatsTo(name);
				var batchers = context.IndexUpdateTriggers.Select(x => x.CreateBatcher(name))
					.Where(x => x != null)
					.ToList();
				foreach (var reduceKey in reduceKeys)
				{
					var entryKey = reduceKey;
					indexWriter.DeleteDocuments(new Term(Abstractions.Data.Constants.ReduceKeyFieldName, entryKey.ToLowerInvariant()));
					batchers.ApplyAndIgnoreAllErrors(
						exception =>
						{
							logIndexing.WarnException(
								string.Format("Error when executed OnIndexEntryDeleted trigger for index '{0}', key: '{1}'",
								              name, entryKey),
								exception);
							context.AddError(name,
							                 entryKey,
							                 exception.Message
								);
						},
						trigger => trigger.OnIndexEntryDeleted(entryKey));
				}
				PropertyDescriptorCollection properties = null;
				var anonymousObjectToLuceneDocumentConverter = new AnonymousObjectToLuceneDocumentConverter(indexDefinition);
				var luceneDoc = new Document();
				var reduceKeyField = new Field(Abstractions.Data.Constants.ReduceKeyFieldName, "dummy",
				                      Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS);
				foreach (var doc in RobustEnumerationReduce(mappedResults, viewGenerator.ReduceDefinition, actions, context))
				{
					count++;
					var fields = GetFields(anonymousObjectToLuceneDocumentConverter, doc, ref properties).ToList();

					string reduceKeyAsString = ExtractReduceKey(viewGenerator, doc);
					reduceKeyField.SetValue(reduceKeyAsString.ToLowerInvariant());

					luceneDoc.GetFields().Clear();
					luceneDoc.Add(reduceKeyField);
					foreach (var field in fields)
					{
						luceneDoc.Add(field);
					}

					batchers.ApplyAndIgnoreAllErrors(
						exception =>
						{
							logIndexing.WarnException(
								string.Format("Error when executed OnIndexEntryCreated trigger for index '{0}', key: '{1}'",
								              name, reduceKeyAsString),
								exception);
							context.AddError(name,
							                 reduceKeyAsString,
							                 exception.Message
								);
						},
						trigger => trigger.OnIndexEntryCreated(reduceKeyAsString, luceneDoc));
					logIndexing.Debug("Reduce key {0} result in index {1} gave document: {2}", reduceKeyAsString, name, luceneDoc);
					AddDocumentToIndex(indexWriter, luceneDoc, analyzer);
					actions.Indexing.IncrementReduceSuccessIndexing();
				}
				batchers.ApplyAndIgnoreAllErrors(
					e =>
					{
						logIndexing.WarnException("Failed to dispose on index update trigger", e);
						context.AddError(name, null, e.Message);
					},
					x => x.Dispose());
				return true;
			});
			logIndexing.Debug(() => string.Format("Reduce resulted in {0} entries for {1} for reduce keys: {2}", count, name,
							  string.Join(", ", reduceKeys)));
		}


		private string ExtractReduceKey(AbstractViewGenerator viewGenerator, object doc)
		{
			try
			{
				dynamic reduceKey = viewGenerator.GroupByExtraction(doc);
				if (reduceKey == null)
				{
					throw new InvalidOperationException("Could not find reduce key for " + name + " in the result: " + doc);
				}
				return ReduceKeyToString(reduceKey);
			}
			catch (Exception e)
			{
				throw new InvalidOperationException("Could not extract reduce key from reduce result!", e);
			}
		}

		private IEnumerable<AbstractField> GetFields(AnonymousObjectToLuceneDocumentConverter anonymousObjectToLuceneDocumentConverter, object doc, ref PropertyDescriptorCollection properties)
		{
			IEnumerable<AbstractField> fields;
			if (doc is DynamicJsonObject)
			{
				fields = anonymousObjectToLuceneDocumentConverter.Index(((DynamicJsonObject)doc).Inner,
																		indexDefinition, Field.Store.YES);
			}
			else
			{
				properties = properties ?? TypeDescriptor.GetProperties(doc);
				fields = anonymousObjectToLuceneDocumentConverter.Index(doc, properties, indexDefinition, Field.Store.YES);
			}
			return fields;
		}
	}
}
