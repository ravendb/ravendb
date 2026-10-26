using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Sparrow.Platform;
using Voron.Data.Tables;
using static Raven.Server.Documents.DocumentsStorage;
using static Raven.Server.Documents.Schemas.Revisions;

namespace Raven.Server.Storage.Schema.Updates.Documents
{
    public class From72002 : ISchemaUpdate
    {
        public int From => 72_002;
        public int To => 72_003;
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Documents;

        internal static int NumberOfRevisionsToScanInSingleTransaction = PlatformDetails.Is32Bits ? 2048 : 32768;

        public bool Update(UpdateStep step)
        {
            var schema = step.DocumentsStorage.RevisionsStorage.RevisionsSchema;
            var startEtag = 0L;
            var done = false;

            while (done == false)
            {
                using (step.AllocateDocumentsOperationContext(out DocumentsOperationContext context))
                {
                    context.TransactionMarkerOffset = (short)step.WriteTx.LowLevelTransaction.Id;

                    step.WriteTx.CreateTree(ConflictRevisionsSlice);

                    var table = new Table(schema, step.ReadTx);
                    var processed = 0;
                    done = true;

                    foreach (var tvr in table.SeekForwardFrom(schema.FixedSizeIndexes[AllRevisionsEtagsSlice], startEtag, skip: 0))
                    {
                        var reader = tvr.Reader;
                        startEtag = TableValueToEtag((int)RevisionsTable.Etag, ref reader) + 1;

                        var flags = TableValueToFlags((int)RevisionsTable.Flags, ref reader);
                        if (flags.Contain(DocumentFlags.Conflicted) || flags.Contain(DocumentFlags.Resolved))
                        {
                            using (TableValueToSlice(context, (int)RevisionsTable.LowerId, ref reader, out var lowerId))
                            using (step.DocumentsStorage.RevisionsStorage.GetKeyPrefix(context, lowerId, out var prefixSlice))
                                RevisionsStorage.AddConflictRevision(context, step.WriteTx, prefixSlice, startEtag - 1);
                        }

                        if (++processed >= NumberOfRevisionsToScanInSingleTransaction)
                        {
                            done = false;
                            break;
                        }
                    }

                    step.Commit(context);
                    step.RenewTransactions();
                }
            }

            return true;
        }
    }
}
