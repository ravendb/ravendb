using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Raven.Database.Storage;

namespace Raven.Database.Indexing
{
    public class JsonToLuceneDocumentConverter
    {
        private readonly IndexWriter indexWriter;
        private readonly DocumentStorageActions actions;

        public JsonToLuceneDocumentConverter(IndexWriter indexWriter, DocumentStorageActions actions)
        {
            this.indexWriter = indexWriter;
            this.actions = actions;
        }

        public int Count { get; private set; }
        public bool ShouldRcreateSearcher { get; set; }

        public void Index(dynamic doc)
        {
            Count++;
            AddValuesToDocument(doc);
            actions.IncrementSuccessIndexing();
        }

        private void AddValuesToDocument(object val)
        {
            var properties = TypeDescriptor.GetProperties(val).Cast<PropertyDescriptor>().ToArray();
            var id = properties.First(x => x.Name == "__document_id");

            var docId = id.GetValue(val).ToString();

            var luceneDoc = new Document();
            luceneDoc.Add(new Field("__document_id", docId, 
                Field.Store.YES, Field.Index.UN_TOKENIZED));
            
            var fields = (from property in properties
                                         where property != id
                                         let value = property.GetValue(val)
                                         where value != null
                                         select new Field(property.Name, ToIndexableString(value), Field.Store.YES, Field.Index.TOKENIZED));

            foreach (var l in fields)
            {
                luceneDoc.Add(l);
            }
            indexWriter.UpdateDocument(new Term("__document_id", docId), luceneDoc);
            ShouldRcreateSearcher = true;
        }


        private static string ToIndexableString(object val)
        {
            if (val is DateTime)
                return DateTools.DateToString((DateTime) val, DateTools.Resolution.DAY);

            return val.ToString();
        }
    }
}