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

namespace Raven.Database.Indexing
{
    public class SimpleIndex : Index
    {
        [CLSCompliant(false)]
        public SimpleIndex(Directory directory, string name, IndexDefinition indexDefinition)
            : base(directory, name, indexDefinition)
        {
        }

        public override void IndexDocuments(AbstractViewGenerator viewGenerator, IEnumerable<object> documents, WorkContext context, IStorageActionsAccessor actions, DateTime minimumTimestamp)
        {
            actions.Indexing.SetCurrentIndexStatsTo(name);
            var count = 0;
            Write(context, indexWriter =>
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
                    indexWriter.DeleteDocuments(new Term("__document_id", documentId));
                    return doc;
                });
                foreach (var doc in RobustEnumeration(documentsWrapped, viewGenerator.MapDefinition, actions, context))
                {
                    count++;

                    string newDocId;
                    IEnumerable<AbstractField> fields;
                    if (doc is DynamicJsonObject)
                        fields = ExtractIndexDataFromDocument((DynamicJsonObject)doc, out newDocId);
                    else
                        fields = ExtractIndexDataFromDocument(properties, doc, out newDocId);

                    if (newDocId != null)
                    {
                        var luceneDoc = new Document();
                        luceneDoc.Add(new Field("__document_id", newDocId, Field.Store.YES, Field.Index.NOT_ANALYZED));

                        madeChanges = true;
                        CopyFieldsToDocument(luceneDoc, fields);
                        batchers.ApplyAndIgnoreAllErrors(
                            exception =>
                            {
                                logIndexing.WarnFormat(exception,
                                                       "Error when executed OnIndexEntryCreated trigger for index '{0}', key: '{1}'",
                                                       name, newDocId);
                                context.AddError(name,
                                            newDocId,
                                            exception.Message
                               );
                            },
                            trigger => trigger.OnIndexEntryCreated(name, newDocId, luceneDoc));
                        logIndexing.DebugFormat("Index '{0}' resulted in: {1}", name, luceneDoc);
                        indexWriter.AddDocument(luceneDoc);
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

        private IEnumerable<AbstractField> ExtractIndexDataFromDocument(DynamicJsonObject dynamicJsonObject, out string newDocId)
        {
            newDocId = dynamicJsonObject.GetDocumentId();
            return AnonymousObjectToLuceneDocumentConverter.Index(dynamicJsonObject.Inner, indexDefinition,
                                                                  Field.Store.NO);
        }

        private IEnumerable<AbstractField> ExtractIndexDataFromDocument(PropertyDescriptorCollection properties, object doc, out string newDocId)
        {
            if (properties == null)
            {
                properties = TypeDescriptor.GetProperties(doc);
            }
            newDocId = properties.Find("__document_id", false).GetValue(doc) as string;
            return AnonymousObjectToLuceneDocumentConverter.Index(doc, properties, indexDefinition, Field.Store.NO);
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
            Write(context, writer =>
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