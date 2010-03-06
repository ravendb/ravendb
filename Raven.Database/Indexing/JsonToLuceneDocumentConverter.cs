using System;
using System.ComponentModel;
using System.Linq;
using Lucene.Net.Documents;

namespace Raven.Database.Indexing
{
    public class JsonToLuceneDocumentConverter
    {
        public Document Index(object val, out string docId)
        {
            var properties = TypeDescriptor.GetProperties(val).Cast<PropertyDescriptor>().ToArray();
            var id = properties.First(x => x.Name == "__document_id");

            docId = id.GetValue(val).ToString();

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
            return luceneDoc;
        }


        private static string ToIndexableString(object val)
        {
            if (val is DateTime)
                return DateTools.DateToString((DateTime) val, DateTools.Resolution.DAY);

            return val.ToString();
        }
    }
}