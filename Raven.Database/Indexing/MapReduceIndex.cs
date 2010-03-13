using System.Collections.Generic;
using System.ComponentModel;
using Lucene.Net.Store;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Linq;
using Raven.Database.Storage;

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
            log.InfoFormat("Mapped {0} documents for {1}", count, name);
        }
    }
}