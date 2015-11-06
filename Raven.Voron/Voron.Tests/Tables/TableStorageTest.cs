using Voron.Data.Tables;

namespace Voron.Tests.Tables
{

    public class TableStorageTest : StorageTest
    {
        protected TableSchema<DocumentsFields> _docsSchema;

        protected override void Configure(StorageEnvironmentOptions options)
        {
            base.Configure(options);

            _docsSchema = new TableSchema<DocumentsFields>("docs")

                .DefineField<long>(DocumentsFields.Etag)
                .DefineField<string>(DocumentsFields.Key)
                .DefineField<string>(DocumentsFields.Data)
                .DefineField<string>(DocumentsFields.Collection)

                .DefineIndex("By/Etag", DocumentsFields.Etag)
                .DefineIndex("By/Etag&Collection", DocumentsFields.Collection, DocumentsFields.Etag)
                .DefineKey(DocumentsFields.Key);
        }

        public enum DocumentsFields
        {
            Etag,
            Key,
            Collection,
            Data,
        }
    }
}