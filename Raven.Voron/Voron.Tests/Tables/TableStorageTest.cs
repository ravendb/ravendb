using Voron.Data.Tables;

namespace Voron.Tests.Tables
{

    public class TableStorageTest : StorageTest
    {
        protected TableSchema<DocumentsFields> _schema;

        protected override void Configure(StorageEnvironmentOptions options)
        {
            base.Configure(options);

            _schema = new TableSchema<DocumentsFields>("docs")

                .DefineField<long>(DocumentsFields.Etag)
                .DefineField<string>(DocumentsFields.Key)
                .DefineField<string>(DocumentsFields.Data)

                .DefineIndex("By/Etag", DocumentsFields.Etag)
                .DefineKey(DocumentsFields.Key);
        }

        public enum DocumentsFields
        {
            Etag,
            Key,
            Data
        }
    }
}