//-----------------------------------------------------------------------
// <copyright file="SimpleIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Raven.Database.Extensions;
using Raven.Database.Linq;
using Raven.Database.Storage;
using Raven.Database.Linq.PrivateExtensions;

namespace Raven.Database.Indexing
{
    public class SimpleIndex : Index
    {
        
        public SimpleIndex(Directory directory, string name, IndexDefinition indexDefinition, AbstractViewGenerator viewGenerator)
            : base(directory, name, indexDefinition, viewGenerator)
        {
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
                    string documentId = doc.__document_id.ToString();
                    if (processedKeys.Add(documentId) == false)
                        return doc;
                    madeChanges = true;
                    batchers.ApplyAndIgnoreAllErrors(
                        exception =>
                        {
                            logIndexing.WarnFormat(exception,
                                                   "Error when executed OnIndexEntryDeleted trigger for index '{0}', key: '{1}'",
                                                   name, documentId);
                            context.AddError(name,
                                             documentId,
                                             exception.Message
                                );
                        },
                        trigger => trigger.OnIndexEntryDeleted(name, documentId));
                    indexWriter.DeleteDocuments(new Term("__document_id", documentId.ToLowerInvariant()));
                    return doc;
                });
                foreach (var doc in RobustEnumerationIndex(documentsWrapped, viewGenerator.MapDefinition, actions, context))
                {
                    count++;

                    IndexingResult indexingResult;
                    if (doc is DynamicJsonObject)
                        indexingResult = ExtractIndexDataFromDocument((DynamicJsonObject)doc);
                    else
                        indexingResult = ExtractIndexDataFromDocument(properties, doc);

                    if (indexingResult.NewDocId != null && indexingResult.ShouldSkip == false)
                    {
                        var luceneDoc = new Document();
                        luceneDoc.Add(new Field("__document_id", indexingResult.NewDocId.ToLowerInvariant(), Field.Store.YES,
                                                Field.Index.NOT_ANALYZED));

                        madeChanges = true;
                        CopyFieldsToDocument(luceneDoc, indexingResult.Fields);
                        batchers.ApplyAndIgnoreAllErrors(
                            exception =>
                            {
                                logIndexing.WarnFormat(exception,
                                                       "Error when executed OnIndexEntryCreated trigger for index '{0}', key: '{1}'",
                                                       name, indexingResult.NewDocId);
                                context.AddError(name,
                                                 indexingResult.NewDocId,
                                                 exception.Message
                                    );
                            },
                            trigger => trigger.OnIndexEntryCreated(name, indexingResult.NewDocId, luceneDoc));
                        logIndexing.DebugFormat("Index '{0}' resulted in: {1}", name, luceneDoc);
                        AddDocumentToIndex(indexWriter, luceneDoc, analyzer);
                    }

                    actions.Indexing.IncrementSuccessIndexing();
                }
                batchers.ApplyAndIgnoreAllErrors(
                    e =>
                    {
                        logIndexing.Warn("Failed to dispose on index update trigger", e);
                        context.AddError(name, null, e.Message);
                    },
                    x => x.Dispose());
                return madeChanges;
            });
            logIndexing.DebugFormat("Indexed {0} documents for {1}", count, name);
        }

        private class IndexingResult
        {
            public string NewDocId;
            public IEnumerable<AbstractField> Fields;
            public bool ShouldSkip;
        }

        private IndexingResult ExtractIndexDataFromDocument(DynamicJsonObject dynamicJsonObject)
        {
        	var newDocId = dynamicJsonObject.GetDocumentId();
        	return new IndexingResult
            {
                Fields = AnonymousObjectToLuceneDocumentConverter.Index(dynamicJsonObject.Inner, indexDefinition,
                                                                  Field.Store.NO),
                NewDocId = newDocId is DynamicNullObject ? null : (string)newDocId,
                ShouldSkip = false
            };
        }

    	private IndexingResult ExtractIndexDataFromDocument(PropertyDescriptorCollection properties, object doc)
        {
            if (properties == null)
            {
                properties = TypeDescriptor.GetProperties(doc);
            }
            var abstractFields = AnonymousObjectToLuceneDocumentConverter.Index(doc, properties, indexDefinition, Field.Store.NO).ToList();
            return new IndexingResult()
            {
                Fields = abstractFields,
                NewDocId = properties.Find("__document_id", false).GetValue(doc) as string,
                ShouldSkip = properties.Count > 1  // we always have at least __document_id
                            && abstractFields.Count == 0
            };
        }

        private static void CopyFieldsToDocument(Document luceneDoc, IEnumerable<AbstractField> fields)
        {
            foreach (var field in fields)
            {
                luceneDoc.Add(field);
            }
        }

        public override void Remove(string[] keys, WorkContext context)
        {
            Write(context, (writer, analyzer) =>
            {
                if (logIndexing.IsDebugEnabled)
                {
                    logIndexing.DebugFormat("Deleting ({0}) from {1}", string.Format(", ", keys), name);
                }
                var batchers = context.IndexUpdateTriggers.Select(x => x.CreateBatcher(name))
                    .Where(x => x != null)
                    .ToList();

                keys.Apply(
                    key => batchers.ApplyAndIgnoreAllErrors(
                        exception =>
                        {
                            logIndexing.WarnFormat(exception,
                                                   "Error when executed OnIndexEntryDeleted trigger for index '{0}', key: '{1}'",
                                                   name, key);
                            context.AddError(name,  key, exception.Message );
                        },
                        trigger => trigger.OnIndexEntryDeleted(name, key)));
                writer.DeleteDocuments(keys.Select(k => new Term("__document_id", k)).ToArray());
                batchers.ApplyAndIgnoreAllErrors(
                    e =>
                    {
                        logIndexing.Warn("Failed to dispose on index update trigger", e);
                        context.AddError(name, null, e.Message );
                    },
                    batcher => batcher.Dispose());
                return true;
            });
        }
    }
}
