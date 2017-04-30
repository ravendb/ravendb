using Raven.Client.Server.PeriodicExport;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.PeriodicExport
{
    public unsafe class PeriodicBackupStore
    {
        private static readonly Slice PeriodicExportStatusSlice;
        

        static PeriodicBackupStore()
        {
            Slice.From(StorageEnvironment.LabelsContext, "PeriodicExportStatus", ByteStringType.Immutable, out PeriodicExportStatusSlice);
        }

        public BlittableJsonReaderObject GetDatabasePeriodicBackupStatus(DocumentsOperationContext context)
        {
            var tree = context.Transaction.InnerTransaction.ReadTree(PeriodicExportStatusSlice);
            var result = tree?.Read(PeriodicExportStatusSlice);
            if (result == null)
                return null;
            return new BlittableJsonReaderObject(result.Reader.Base, result.Reader.Length, context);
        }

        public void SetDatabasePeriodicBackupStatus(DocumentsOperationContext context, PeriodicExportStatus periodicExportStatus)
        {
            var jsonVal = periodicExportStatus.ToJson();
            var tree = context.Transaction.InnerTransaction.CreateTree(PeriodicExportStatusSlice);
            using (var json = context.ReadObject(jsonVal, "backup status"))
            using (tree.DirectAdd(PeriodicExportStatusSlice, json.Size, out byte* dest))
            {
                json.CopyTo(dest);
            }
        }
    }
}
