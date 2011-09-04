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
using Raven.Database.Extensions;
using Raven.Database.Linq;
using Raven.Database.Storage;

namespace Raven.Database.Indexing
{
	public class SimpleIndex : Index
	{
		
		public SimpleIndex(Directory directory, string name, IndexDefinition indexDefinition, AbstractViewGenerator viewGenerator)
			: base(directory, name, indexDefinition, viewGenerator)
		{
		}

		public override bool IsMapReduce
		{
			get { return false; }
		}

		public override void IndexDocuments(AbstractViewGenerator viewGenerator, IEnumerable<object> documents, WorkContext context, IStorageActionsAccessor actions, DateTime minimumTimestamp)
		{
			actions.Indexing.SetCurrentIndexStatsTo(name);
			var count = 0;
			Write(context, (indexWriter, analyzer) =>
			{
				bool madeChanges = false;
				PropertyDescriptorCollection properties = null;
				var processedKeys = new HashSet<string>();
				var batchers = context.IndexUpdateTriggers.Select(x => x.CreateBatcher(name))
					.Where(x => x != null)
					.ToList();
				var documentsWrapped = documents.Select((dynamic doc) =>
				{
					if(doc.__document_id == null)
						throw new ArgumentException("Cannot index something which doesn't have a document id, but got: " + doc);

					string documentId = doc.__document_id.ToString();
					if (processedKeys.Add(documentId) == false)
						return doc;
					madeChanges = true;
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
				});
				var anonymousObjectToLuceneDocumentConverter = new AnonymousObjectToLuceneDocumentConverter(indexDefinition);
				var luceneDoc = new Document();
				var documentIdField = new Field(Constants.DocumentIdFieldName, "dummy", Field.Store.YES,
											  Field.Index.NOT_ANALYZED_NO_NORMS);
				foreach (var doc in RobustEnumerationIndex(documentsWrapped, viewGenerator.MapDefinitions, actions, context))
				{
					count++;

					IndexingResult indexingResult;
					if (doc is DynamicJsonObject)
						indexingResult = ExtractIndexDataFromDocument(anonymousObjectToLuceneDocumentConverter, (DynamicJsonObject)doc);
					else
						indexingResult = ExtractIndexDataFromDocument(anonymousObjectToLuceneDocumentConverter, properties, doc);

					if (indexingResult.NewDocId != null && indexingResult.ShouldSkip == false)
					{
						


						madeChanges = true;
						luceneDoc.GetFields().Clear();
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
									string.Format( "Error when executed OnIndexEntryCreated trigger for index '{0}', key: '{1}'",
													   name, indexingResult.NewDocId),
									exception);
								context.AddError(name,
												 indexingResult.NewDocId,
												 exception.Message
									);
							},
							trigger => trigger.OnIndexEntryCreated(indexingResult.NewDocId, luceneDoc));
						logIndexing.Debug("Index '{0}' resulted in: {1}", name, luceneDoc);
						AddDocumentToIndex(indexWriter, luceneDoc, analyzer);
					}

					actions.Indexing.IncrementSuccessIndexing();
				}
				batchers.ApplyAndIgnoreAllErrors(
					e =>
					{
						logIndexing.WarnException("Failed to dispose on index update trigger", e);
						context.AddError(name, null, e.Message);
					},
					x => x.Dispose());
				return madeChanges;
			});
			logIndexing.Debug("Indexed {0} documents for {1}", count, name);
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
				Fields = anonymousObjectToLuceneDocumentConverter.Index(dynamicJsonObject.Inner, indexDefinition,
																  Field.Store.NO).ToList(),
				NewDocId = newDocId is DynamicNullObject ? null : (string)newDocId,
				ShouldSkip = false
			};
		}

		private IndexingResult ExtractIndexDataFromDocument(AnonymousObjectToLuceneDocumentConverter anonymousObjectToLuceneDocumentConverter, PropertyDescriptorCollection properties, object doc)
		{
			if (properties == null)
			{
				properties = TypeDescriptor.GetProperties(doc);
			}
			var abstractFields = anonymousObjectToLuceneDocumentConverter.Index(doc, properties, indexDefinition, Field.Store.NO).ToList();
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
			Write(context, (writer, analyzer) =>
			{
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
				writer.DeleteDocuments(keys.Select(k => new Term(Constants.DocumentIdFieldName, k)).ToArray());
				batchers.ApplyAndIgnoreAllErrors(
					e =>
					{
						logIndexing.WarnException("Failed to dispose on index update trigger", e);
						context.AddError(name, null, e.Message);
					},
					batcher => batcher.Dispose());
				return true;
			});
		}
	}
}
