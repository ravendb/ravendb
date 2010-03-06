using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Lucene.Net.Documents;

namespace Raven.Database.Indexing
{
    public class JsonToLuceneDocumentConverter
    {
        public IEnumerable<Field> Index(object val, out string docId)
        {
            var properties = TypeDescriptor.GetProperties(val).Cast<PropertyDescriptor>().ToArray();
            var id = properties.First(x => x.Name == "__document_id");

            docId = id.GetValue(val).ToString();

            return (from property in properties
                    where property != id
                    let value = property.GetValue(val)
                    where value != null
                    select new Field(property.Name, ToIndexableString(value), Field.Store.YES, Field.Index.TOKENIZED));
        }


        private static string ToIndexableString(object val)
        {
            if (val is DateTime)
                return DateTools.DateToString((DateTime) val, DateTools.Resolution.DAY);

            return val.ToString();
        }
    }
}