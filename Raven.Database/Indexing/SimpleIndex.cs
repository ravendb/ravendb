//-----------------------------------------------------------------------
// <copyright file="SimpleIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Linq;
using Raven.Database.Storage;

namespace Raven.Database.Indexing
{
	public class SimpleIndex : Index
	{
		public SimpleIndex(Directory directory, string name, IndexDefinition indexDefinition, AbstractViewGenerator viewGenerator, InMemoryRavenConfiguration configuration)
			: base(directory, name, indexDefinition, viewGenerator, configuration)
		{
		}

		public override bool IsMapReduce
		{
			get { return false; }
		}

		public override void IndexDocuments(AbstractViewGenerator viewGenerator, IEnumerable<object> documents, WorkContext context, IStorageActionsAccessor actions, DateTime minimumTimestamp)
		{
			var count = 0;
			Write(context, (indexWriter, analyzer, stats) =>
			{
				var processedKeys = new HashSet<string>();
				var batchers = context.IndexUpdateTriggers.Select(x => x.CreateBatcher(name))
					.Where(x => x != null)
					.ToList();
				try
				{
					var documentsWrapped = documents.Select((dynamic doc) =>
					{
						if (doc.__document_id == null)
							throw new ArgumentException(string.Format("Cannot index something which doesn't have a document id, but got: '{0}'", doc));

						count++;
						string documentId = doc.__document_id.ToString();
						if (processedKeys.Add(documentId) == false)
							return doc;
						batchers.ApplyAndIgnoreAllErrors(
							exception =>
							{
								logIndexing.WarnException(
									string.Format("Error when executed OnIndexEntryDeleted trigger for index '{0}', key: '{1}'",
													   name, documentId),
									exception);
								context.AddError(name,
												 documentId,
												 exception.Message
									);
							},
							trigger => trigger.OnIndexEntryDeleted(documentId));
						indexWriter.DeleteDocuments(new Term(Constants.DocumentIdFieldName, documentId.ToLowerInvariant()));
						return doc;
					})
					.Where(x => x is FilteredDocument == false);
					var anonymousObjectToLuceneDocumentConverter = new AnonymousObjectToLuceneDocumentConverter(indexDefinition);
					var luceneDoc = new Document();
					var documentIdField = new Field(Constants.DocumentIdFieldName, "dummy", Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS);
					foreach (var doc in RobustEnumerationIndex(documentsWrapped, viewGenerator.MapDefinitions, actions, context, stats))
					{

						count++;

						float boost;
						var indexingResult = GetIndexingResult(doc, anonymousObjectToLuceneDocumentConverter, out boost);

						if (indexingResult.NewDocId != null && indexingResult.ShouldSkip == false)
						{
							count += 1;
							luceneDoc.GetFields().Clear();
							luceneDoc.SetBoost(boost);
							documentIdField.SetValue(indexingResult.NewDocId.ToLowerInvariant());
							luceneDoc.Add(documentIdField);
							foreach (var field in indexingResult.Fields)
							{
								luceneDoc.Add(field);
							}
							batchers.ApplyAndIgnoreAllErrors(
								exception =>
								{
									logIndexing.WarnException(
										string.Format("Error when executed OnIndexEntryCreated trigger for index '{0}', key: '{1}'",
														   name, indexingResult.NewDocId),
										exception);
									context.AddError(name,
													 indexingResult.NewDocId,
													 exception.Message
										);
								},
								trigger => trigger.OnIndexEntryCreated(indexingResult.NewDocId, luceneDoc));
							LogIndexedDocument(indexingResult.NewDocId, luceneDoc);
							AddDocumentToIndex(indexWriter, luceneDoc, analyzer);
						}

						stats.IndexingSuccesses++;
					}
				}
				catch(Exception e)
				{

					batchers.ApplyAndIgnoreAllErrors(
						ex =>
						{
							logIndexing.WarnException("Failed to notify index update trigger batcher about an error", ex);
							context.AddError(name, null, ex.Message);
						},
						x => x.AnErrorOccured(e));
					throw;
				}
				finally
				{
					batchers.ApplyAndIgnoreAllErrors(
						e =>
						{
							logIndexing.WarnException("Failed to dispose on index update trigger", e);
							context.AddError(name, null, e.Message);
						},
						x => x.Dispose());
				}
				return count;
			});
			logIndexing.Debug("Indexed {0} documents for {1}", count, name);
		}

		private IndexingResult GetIndexingResult(object doc, AnonymousObjectToLuceneDocumentConverter anonymousObjectToLuceneDocumentConverter, out float boost)
		{
			boost = 1;

			var boostedValue = doc as BoostedValue;
			if (boostedValue != null)
			{
				doc = boostedValue.Value;
				boost = boostedValue.Boost;
			}

			IndexingResult indexingResult;
			if (doc is DynamicJsonObject)
				indexingResult = ExtractIndexDataFromDocument(anonymousObjectToLuceneDocumentConverter, (DynamicJsonObject) doc);
			else
				indexingResult = ExtractIndexDataFromDocument(anonymousObjectToLuceneDocumentConverter, doc);

			if (Math.Abs(boost - 1) > float.Epsilon)
			{
				foreach (var abstractField in indexingResult.Fields)
				{
					abstractField.SetOmitNorms(false);
				}
			}

			return indexingResult;
		}

		private class IndexingResult
		{
			public string NewDocId;
			public List<AbstractField> Fields;
			public bool ShouldSkip;
		}

		private IndexingResult ExtractIndexDataFromDocument(AnonymousObjectToLuceneDocumentConverter anonymousObjectToLuceneDocumentConverter, DynamicJsonObject dynamicJsonObject)
		{
			var newDocId = dynamicJsonObject.GetDocumentId();
			return new IndexingResult
			{
				Fields = anonymousObjectToLuceneDocumentConverter.Index(((IDynamicJsonObject)dynamicJsonObject).Inner, Field.Store.NO).ToList(),
				NewDocId = newDocId is DynamicNullObject ? null : (string)newDocId,
				ShouldSkip = false
			};
		}

		private readonly Dictionary<Type, PropertyDescriptorCollection> propertyDescriptorCache = new Dictionary<Type, PropertyDescriptorCollection>();

		private IndexingResult ExtractIndexDataFromDocument(AnonymousObjectToLuceneDocumentConverter anonymousObjectToLuceneDocumentConverter, object doc)
		{
		    PropertyDescriptorCollection properties;
			Type type = doc.GetType();
			if(propertyDescriptorCache.TryGetValue(type, out properties) == false)
			{
				propertyDescriptorCache[type] = properties = TypeDescriptor.GetProperties(doc);
			}
			
			var abstractFields = anonymousObjectToLuceneDocumentConverter.Index(doc, properties, Field.Store.NO).ToList();
			return new IndexingResult()
			{
				Fields = abstractFields,
				NewDocId = properties.Find(Constants.DocumentIdFieldName, false).GetValue(doc) as string,
				ShouldSkip = properties.Count > 1  // we always have at least __document_id
							&& abstractFields.Count == 0
			};
		}


		public override void Remove(string[] keys, WorkContext context)
		{
			Write(context, (writer, analyzer,stats) =>
			{
				stats.Operation = IndexingWorkStats.Status.Ignore;
				logIndexing.Debug(() => string.Format("Deleting ({0}) from {1}", string.Join(", ", keys), name));
				var batchers = context.IndexUpdateTriggers.Select(x => x.CreateBatcher(name))
					.Where(x => x != null)
					.ToList();

				keys.Apply(
					key => batchers.ApplyAndIgnoreAllErrors(
						exception =>
						{
							logIndexing.WarnException(
								string.Format("Error when executed OnIndexEntryDeleted trigger for index '{0}', key: '{1}'",
								              name, key),
								exception);
							context.AddError(name, key, exception.Message);
						},
						trigger => trigger.OnIndexEntryDeleted(key)));
				writer.DeleteDocuments(keys.Select(k => new Term(Constants.DocumentIdFieldName, k.ToLowerInvariant())).ToArray());
				batchers.ApplyAndIgnoreAllErrors(
					e =>
					{
						logIndexing.WarnException("Failed to dispose on index update trigger", e);
						context.AddError(name, null, e.Message);
					},
					batcher => batcher.Dispose());
				return keys.Length;
			});
		}
	}
}
