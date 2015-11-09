using Bond;
using Voron.Data.Tables;

namespace Voron.Tests.Tables
{

    public class TableStorageTest : StorageTest
    {
        //protected TableSchema<DocumentsFields> _docsSchema;
        protected TableSchema<Documents> _docsSchema;

        protected override void Configure(StorageEnvironmentOptions options)
        {
            base.Configure(options);

            //_docsSchema = new TableSchema<DocumentsFields>("docs")

            //    .DefineField<long>(DocumentsFields.Etag)
            //    .DefineField<string>(DocumentsFields.Key)
            //    .DefineField<string>(DocumentsFields.Data)
            //    .DefineField<string>(DocumentsFields.Collection)

            //    .DefineIndex("By/Etag", DocumentsFields.Etag)
            //    .DefineIndex("By/Etag&Collection", DocumentsFields.Collection, DocumentsFields.Etag)
            //    .DefineKey(DocumentsFields.Key);

            _docsSchema = new TableSchema<Documents>("docs")                
                .DefineIndex("By/Etag", x => x.Etag)
                .DefineIndex("By/Etag&Collection", x => x.Collection, x => x.Etag)
                .DefineKey(x => x.Key);
        }

        public sealed class Documents
        {
            public long Etag;
            public string Key;
            public string Collection;
            public IBonded<string> Data;
        }

        //public enum DocumentsFields
        //{
        //    Etag,
        //    Key,
        //    Collection,
        //    Data,
        //}
    }
}