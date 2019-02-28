using System;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using Raven.Client;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Storage.Schema.Updates.Server
{
    public class From11 : ISchemaUpdate
    {
        private static readonly TableSchema ItemsSchema;
        private static readonly TableSchema CertificatesSchema;
        private static readonly Slice Items;
        private static readonly Slice CertificatesSlice;
        private static readonly Slice CertificatesHashSlice;

        public enum CertificatesTable
        {
            Key = 0,
            Hash = 1,
            Data = 2
        }

        static From11()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "Items", out Items);
                Slice.From(ctx, "CertificatesSlice", out CertificatesSlice);
                Slice.From(ctx, "CertificatesHashSlice", out CertificatesHashSlice);
            }

            ItemsSchema = new TableSchema();

            ItemsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1
            });

            CertificatesSchema = new TableSchema();
            CertificatesSchema.DefineKey(new TableSchema.SchemaIndexDef()
            {
                StartIndex = (int)CertificatesTable.Key,
                Count = 1,
                IsGlobal = false,
                Name = CertificatesSlice
            });
            CertificatesSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)CertificatesTable.Hash,
                Count = 1,
                IsGlobal = false,
                Name = CertificatesHashSlice
            });
        }

        public bool Update(UpdateStep step)
        {
            ItemsSchema.Create(step.WriteTx, Items, 32);
            var itemsTable = step.WriteTx.OpenTable(ItemsSchema, Items);

            CertificatesSchema.Create(step.WriteTx, CertificatesSlice, 32);
            var certsTable = step.WriteTx.OpenTable(CertificatesSchema, CertificatesSlice);

            using (step.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                // First we'll update all the certs in the local state with the pinning hash
                var localCertKeys = step.ServerStore.Cluster.GetCertificateKeysFromLocalState(step.ReadTx).ToList();

                foreach (var localCertKey in localCertKeys)
                {
                    using (var localCertificate = step.ServerStore.Cluster.GetLocalState(step.ReadTx, context, localCertKey))
                    {
                        if (localCertificate == null)
                            continue;

                        var def = JsonDeserializationServer.CertificateDefinition(localCertificate);
                        def.PublicKeyPinningHash = CertificateUtils.GetPublicKeyPinningHash(new X509Certificate2(Convert.FromBase64String(def.Certificate)));

                        var cert = context.ReadObject(def.ToJson(), "updated/certificate");

                        step.ServerStore.Cluster.PutLocalState(step.WriteTx, context, localCertKey, cert);
                    }
                }

                // Read all the certs from the items table, add the pinning hash and store them in the new table. Then delete the original.

                var allClusterCerts = step.ServerStore.Cluster.ItemsStartingWith(context, Constants.Certificates.Prefix, 0, int.MaxValue);

                foreach (var cert in allClusterCerts)
                {
                    var def = JsonDeserializationServer.CertificateDefinition(cert.Value);
                    def.PublicKeyPinningHash = CertificateUtils.GetPublicKeyPinningHash(new X509Certificate2(Convert.FromBase64String(def.Certificate)));
                                        
                    using (Slice.From(context.Allocator, def.PublicKeyPinningHash, out Slice hashSlice))
                    using (Slice.From(context.Allocator, cert.ItemName, out Slice keySlice))
                    using (var newCert = context.ReadObject(def.ToJson(), "updated/certificate"))
                    {
                        ClusterStateMachine.UpdateCertificate(certsTable, keySlice, hashSlice, newCert);
                        itemsTable.DeleteByKey(keySlice);
                    }
                }
            }

            return true;
        }
    }
}
