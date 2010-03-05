using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Lucene.Net.Documents;
using Lucene.Net.Index;

namespace Raven.Database.Indexing
{
    public class JsonToLuceneDocumentConverter
    {
        private readonly IndexWriter indexWriter;
        private Document luceneDoc;

        public JsonToLuceneDocumentConverter(IndexWriter indexWriter)
        {
            CurrentId = Guid.NewGuid().ToString();
            this.indexWriter = indexWriter;
        }

        public int Count { get; private set; }
        public bool ShouldRcreateSearcher { get; set; }

        public string CurrentId { get; private set; }

        public void Index(dynamic doc)
        {
            Count++;
            var fields = new List<Field>();
            string docId = AddValuesToDocument(fields, doc);
            if (docId != CurrentId)
            {
                FlushDocumentToLuceneIndex(docId);
                CurrentId = docId;
                luceneDoc = new Document();
                luceneDoc.Add(new Field("__document_id", docId, Field.Store.YES, Field.Index.UN_TOKENIZED));
            }
            foreach (var field in fields)
            {
                luceneDoc.Add(field);
            }
        }

        public void FlushDocumentIfNeeded()
        {
            FlushDocumentToLuceneIndex(CurrentId);
        }

        private void FlushDocumentToLuceneIndex(string docId)
        {
            if (luceneDoc == null)
                return;

            if (luceneDoc.GetFieldsCount() <= 0) 
                return;
            
            indexWriter.UpdateDocument(new Term("__document_id", docId), luceneDoc);
            ShouldRcreateSearcher = true;
        }

        private static string AddValuesToDocument(ICollection<Field> fields, object val)
        {
            PropertyDescriptor[] properties = TypeDescriptor.GetProperties(val).Cast<PropertyDescriptor>().ToArray();
            PropertyDescriptor id = properties.First(x => x.Name == "__document_id");

            foreach (PropertyDescriptor property in properties)
            {
                if (property == id)
                    continue;
                object value = property.GetValue(val);
                if (value == null)
                    continue;
                fields.Add(new Field(property.Name, ToIndexableString(value),
                                     Field.Store.YES,
                                     Field.Index.TOKENIZED));
            }
            return (string) id.GetValue(val);
        }


        private static string ToIndexableString(object val)
        {
            if (val is DateTime)
                return DateTools.DateToString((DateTime) val, DateTools.Resolution.DAY);

            return val.ToString();
        }
    }
}