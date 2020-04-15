using System.Text;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Storage.Schema.Updates.Server
{
    public unsafe class From42015 : ISchemaUpdate
    {
        public int From => 42_015;

        public int To => 42_016;

        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Server;

        public bool Update(UpdateStep step)
        {
            return UpdateCertificatesTableInternal(step);
        }

        internal static bool UpdateCertificatesTableInternal(UpdateStep step)
        {
            var schema = ClusterStateMachine.CertificatesSchema;

            var readCertsTable = step.ReadTx.OpenTable(schema, ClusterStateMachine.CertificatesSlice);
            var writeCertsTable = step.WriteTx.OpenTable(schema, ClusterStateMachine.CertificatesSlice);

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                foreach (var cert in readCertsTable.SeekByPrimaryKey(Slices.Empty, 0))
                {
                    (string key, BlittableJsonReaderObject doc) = GetCurrentItem(step.WriteTx, context, cert);

                    using (doc)
                    {
                        var def = JsonDeserializationServer.CertificateDefinition(doc);
                        From40011.DropCertificatePrefixFromDefinition(def, out var touched);

                        var loweredKey = key.ToLowerInvariant();

                        if (loweredKey != key)
                        {
                            // we have upper cased key (thumbprint)
                            // let's remove current record from table and force writing it again with lower cased key value

                            using (Slice.From(step.WriteTx.Allocator, key, out Slice keySlice))
                                writeCertsTable.DeleteByKey(keySlice);

                            touched = true;
                        }

                        if (touched)
                        {
                            using (Slice.From(step.WriteTx.Allocator, def.PublicKeyPinningHash, out Slice hashSlice))
                            using (Slice.From(step.WriteTx.Allocator, loweredKey, out Slice keySlice))
                            using (var newCert = context.ReadObject(def.ToJson(), "certificate/updated"))
                            {
                                ClusterStateMachine.UpdateCertificate(writeCertsTable, keySlice, hashSlice, newCert);
                            }
                        }
                    }
                }
            }

            return true;
        }

        private static (string, BlittableJsonReaderObject) GetCurrentItem(Transaction tx, JsonOperationContext context, Table.TableValueHolder result)
        {
            var ptr = result.Reader.Read((int)ClusterStateMachine.CertificatesTable.Data, out int size);
            var doc = new BlittableJsonReaderObject(ptr, size, context);
            var key = Encoding.UTF8.GetString(result.Reader.Read((int)ClusterStateMachine.CertificatesTable.Thumbprint, out size), size);

            Transaction.DebugDisposeReaderAfterTransaction(tx, doc);
            return (key, doc);
        }
    }
}
