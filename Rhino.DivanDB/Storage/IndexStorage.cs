using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Runtime.ConstrainedExecution;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Rhino.DivanDB.Json;
using Rhino.DivanDB.Linq;
using Directory = System.IO.Directory;
using System.Linq;

namespace Rhino.DivanDB.Storage
{
    public class IndexStorage : CriticalFinalizerObject,IDisposable
    {
        private readonly string path;
        private readonly FSDirectory directory;

        public IndexStorage(string path)
        {
            this.path = Path.Combine(path, "Index");

            var create = Directory.Exists(this.path) == false;

            directory = FSDirectory.GetDirectory(this.path, create);
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
            directory.Close();
        }

        public void Delete(string key)
        {
            var indexWriter = new IndexWriter(directory, new StandardAnalyzer());
            try
            {
                indexWriter.DeleteDocuments(new Term("_id", key));
            }
            finally
            {
                indexWriter.Close();
            }
        }

        public void Index(ViewFunc func, IEnumerable<JsonDynamicObject> input)
        {
            var docs = func(input);
            var indexWriter = new IndexWriter(directory, new StandardAnalyzer());
            try
            {
                Document luceneDoc = docs.Cast<object>().Aggregate<object, Document>(null, (current, doc) => CreateOrEditDoc(indexWriter, current, doc));
                if (luceneDoc != null)
                {
                    indexWriter.DeleteDocuments(new Term("_id", luceneDoc.GetField("_id").StringValue()));
                    indexWriter.AddDocument(luceneDoc);
                }
                indexWriter.Flush();
            }
            finally
            {
                indexWriter.Close();
            }
        }

        private static Document CreateOrEditDoc(IndexWriter writer,Document doc, object val)
        {
            var properties = TypeDescriptor.GetProperties(val).Cast<PropertyDescriptor>().ToArray();
            var id = properties.First(x => x.Name == "_id");
            var idVal = id.GetValue(val).ToString();
            if (doc == null || doc.GetField("_id").StringValue() != idVal)
            {
                if (doc != null)
                {
                    writer.DeleteDocuments(new Term("_id", idVal)); 
                    writer.AddDocument(doc);
                }
                doc = new Document();
                doc.Add(new Field("_id", idVal, Field.Store.YES, Field.Index.UN_TOKENIZED));
            }
            foreach (PropertyDescriptor property in properties)
            {
                if(property == id)
                    continue;
                doc.Add(new Field(property.Name, ToIndexableString(property.GetValue(val)), 
                    Field.Store.YES, 
                    Field.Index.TOKENIZED));
            }
            return doc;
        }

        private static string ToIndexableString(object val)
        {
            if (val is string)
                return val.ToString();

            if (val is DateTime)
                return DateTools.DateToString((DateTime)val, DateTools.Resolution.DAY);

            return val.ToString();
        }

        ~IndexStorage()
        {
            directory.Close();
        }
    }
}