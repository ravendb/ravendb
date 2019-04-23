using System.Linq;
using System.Text;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Storage.Schema.Updates.Server
{
    public unsafe class From15 : ISchemaUpdate
    {
        public bool Update(UpdateStep step)
        {
            var schema = ClusterStateMachine.CertificatesSchema;
            var certsTable = step.WriteTx.OpenTable(schema, ClusterStateMachine.CertificatesSlice);

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var certificates = certsTable.SeekByPrimaryKey(Slices.Empty, 0).ToList();
            
                foreach (var cert in certificates)
                {
                    (string key, BlittableJsonReaderObject doc) = GetCurrentItem(step.WriteTx, context, cert);
                    using (doc)
                    {
                        var def = JsonDeserializationServer.CertificateDefinition(doc);
                        From11.DropCertificatePrefixFromDefinition(def, out var touched);

                        if (touched)
                        {
                            using (Slice.From(step.WriteTx.Allocator, def.PublicKeyPinningHash, out Slice hashSlice))
                            using (Slice.From(step.WriteTx.Allocator, key, out Slice keySlice))
                            using (var newCert = context.ReadObject(def.ToJson(), "certificate/updated"))
                            {
                                ClusterStateMachine.UpdateCertificate(certsTable, keySlice, hashSlice, newCert);
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
