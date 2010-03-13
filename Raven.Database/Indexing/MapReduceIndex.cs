using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
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
                    PropertyDescriptorCollection props = TypeDescriptor.GetProperties(doc);
                    groupByPropertyDescriptor = props.Find(viewGenerator.GroupByField, false);
                    documentIdPropertyDescriptor = props.Find("__document_id", false);
                }

                var reduceKey = groupByPropertyDescriptor.GetValue(doc) as string;
                var docId = documentIdPropertyDescriptor.GetValue(doc) as string;

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
                    if(properties==null)
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

                return count > 0;
            });
            log.DebugFormat("Reduce resulted in {0} entires for {1} for reduce key {2}", count, name, reduceKey);
        }
    }
}