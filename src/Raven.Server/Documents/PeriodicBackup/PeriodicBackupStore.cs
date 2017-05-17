using Raven.Client.Server.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.PeriodicBackup
{
    public unsafe class PeriodicBackupStore
    {
        private static readonly Slice PeriodicExportStatusSlice;
        

        static PeriodicBackupStore()
        {
            Slice.From(StorageEnvironment.LabelsContext, "PeriodicBackupStatus", ByteStringType.Immutable, out PeriodicExportStatusSlice);
        }

        public BlittableJsonReaderObject GetDatabasePeriodicBackupStatus(DocumentsOperationContext context)
        {
            var tree = context.Transaction.InnerTransaction.ReadTree(PeriodicExportStatusSlice);
            var result = tree?.Read(PeriodicExportStatusSlice);
            if (result == null)
                return null;
            return new BlittableJsonReaderObject(result.Reader.Base, result.Reader.Length, context);
        }

        public void SetDatabasePeriodicBackupStatus(DocumentsOperationContext context, PeriodicBackupStatus periodicBackupStatus)
        {
            var jsonVal = periodicBackupStatus.ToJson();
            var tree = context.Transaction.InnerTransaction.CreateTree(PeriodicExportStatusSlice);
            using (var json = context.ReadObject(jsonVal, "backup status"))
            using (tree.DirectAdd(PeriodicExportStatusSlice, json.Size, out byte* dest))
            {
                json.CopyTo(dest);
            }
        }
    }
}
