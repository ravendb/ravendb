using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Storage.Schema.Updates.Server
{
    public sealed class From40011 : ISchemaUpdate
    {
        private const string LocalNodeStateTreeName = "LocalNodeState";

        private static readonly TableSchema ItemsSchema;
        private static readonly TableSchema CertificatesSchema;
        private static readonly Slice Items;
        private static readonly Slice CertificatesSlice;
        private static readonly Slice CertificatesHashSlice;

        public int From => 40_011;

        public int To => 42_012;

        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Server;

        public enum CertificatesTable
        {
            Key = 0,
            Hash = 1,
            Data = 2
        }

        static From40011()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "Items", out Items);
                Slice.From(ctx, "CertificatesSlice", out CertificatesSlice);
                Slice.From(ctx, "CertificatesHashSlice", out CertificatesHashSlice);
            }

            ItemsSchema = new TableSchema();

            ItemsSchema.DefineKey(new TableSchema.IndexDef
            {
                StartIndex = 0,
                Count = 1
            });

            CertificatesSchema = new TableSchema();
            CertificatesSchema.DefineKey(new TableSchema.IndexDef()
            {
                StartIndex = (int)CertificatesTable.Key,
                Count = 1,
                IsGlobal = false,
                Name = CertificatesSlice
            });
            CertificatesSchema.DefineIndex(new TableSchema.IndexDef
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

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                // First we'll update all the certs in the local state with the pinning hash
                var localCertKeys = GetCertificateKeysFromLocalState(step.ReadTx).ToList();

                foreach (var localCertKey in localCertKeys)
                {
                    using (var localCertificate = GetLocalStateByThumbprint(step.ReadTx, context, localCertKey))
                    {
                        if (localCertificate == null)
                            continue;

                        using (localCertificate)
                        {
                            var def = JsonDeserializationServer.CertificateDefinition(localCertificate);
                            def.PublicKeyPinningHash = CertificateLoaderUtil.CreateCertificateFromAny(Convert.FromBase64String(def.Certificate)).GetPublicKeyPinningHash();

                            using (var cert = context.ReadObject(def.ToJson(), "updated/certificate"))
                                PutLocalState(step.WriteTx, localCertKey, cert);
                        }
                    }
                }

                // Read all the certs from the items table, add the pinning hash and store them in the new table. Then delete the original.
                var allClusterCerts = ItemsStartingWith(step.WriteTx, context, step.WriteTx.Allocator, Constants.Certificates.Prefix, 0, int.MaxValue).ToList();

                foreach (var cert in allClusterCerts)
                {
                    using (cert.Value)
                    {
                        var def = JsonDeserializationServer.CertificateDefinition(cert.Value);
                        def.PublicKeyPinningHash = CertificateLoaderUtil.CreateCertificateFromAny(Convert.FromBase64String(def.Certificate)).GetPublicKeyPinningHash();

                        using (Slice.From(step.WriteTx.Allocator, def.PublicKeyPinningHash, out Slice hashSlice))
                        using (Slice.From(step.WriteTx.Allocator, cert.ItemName, out Slice oldKeySlice)) // includes the 'certificates/' prefix
                        using (Slice.From(step.WriteTx.Allocator, def.Thumbprint.ToLowerInvariant(), out Slice newKeySlice))
                        {
                            // in this update we trim 'certificates/' prefix from key name, CollectionPrimaryKey and CollectionSecondaryKeys
                            DropCertificatePrefixFromDefinition(def, out _);

                            using (var newCert = context.ReadObject(def.ToJson(), "certificate/new/schema"))
                            {
                                ClusterStateMachine.UpdateCertificate(certsTable, newKeySlice, hashSlice, newCert);
                                itemsTable.DeleteByKey(oldKeySlice);
                            }
                        }
                    }
                }
            }

            return true;
        }

        public IEnumerable<string> GetCertificateKeysFromLocalState(Transaction tx)
        {
            var tree = tx.ReadTree(LocalNodeStateTreeName);
            if (tree == null)
                yield break;

            using (var it = tree.Iterate(prefetch: false))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    yield break;
                do
                {
                    yield return it.CurrentKey.ToString();
                } while (it.MoveNext());
            }
        }

        public unsafe BlittableJsonReaderObject GetLocalStateByThumbprint(Transaction tx, JsonOperationContext context, string key)
        {
            var localState = tx.ReadTree(LocalNodeStateTreeName);
            var read = localState.Read(key);
            if (read == null)
                return null;
            BlittableJsonReaderObject localStateBlittable = new BlittableJsonReaderObject(read.Reader.Base, read.Reader.Length, context);

            Transaction.DebugDisposeReaderAfterTransaction(tx, localStateBlittable);
            return localStateBlittable;
        }

        public unsafe void PutLocalState(Transaction tx, string key, BlittableJsonReaderObject value)
        {
            var localState = tx.CreateTree(LocalNodeStateTreeName);
            using (localState.DirectAdd(key, value.Size, out var ptr))
            {
                value.CopyTo(ptr);
            }
        }

        private IEnumerable<(string ItemName, BlittableJsonReaderObject Value)> ItemsStartingWith(Transaction tx, JsonOperationContext context, ByteStringContext allocator, string prefix, int start, int take)
        {
            var items = tx.OpenTable(ItemsSchema, Items);

            var dbKey = prefix.ToLowerInvariant();
            using (Slice.From(allocator, dbKey, out Slice loweredPrefix))
            {
                foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, start))
                {
                    if (take-- <= 0)
                        yield break;

                    yield return GetCurrentItem(tx, context, result.Value);
                }
            }
        }

        private static unsafe (string, BlittableJsonReaderObject) GetCurrentItem(Transaction tx, JsonOperationContext context, Table.TableValueHolder result)
        {
            var ptr = result.Reader.Read(2, out int size);
            var doc = new BlittableJsonReaderObject(ptr, size, context);
            var key = Encoding.UTF8.GetString(result.Reader.Read(1, out size), size);

            Transaction.DebugDisposeReaderAfterTransaction(tx, doc);
            return (key, doc);
        }

        public static void DropCertificatePrefixFromDefinition(CertificateDefinition definition, out bool touched)
        {
            touched = false;

            if (definition.CollectionSecondaryKeys != null)
            {
                var secondaryKeys = new List<string>();
                foreach (var secondaryKey in definition.CollectionSecondaryKeys)
                {
                    if (secondaryKey.StartsWith(Constants.Certificates.Prefix))
                    {
                        touched = true;
                        secondaryKeys.Add(secondaryKey.Substring(Constants.Certificates.Prefix.Length));
                    }
                    else
                    {
                        secondaryKeys.Add(secondaryKey);
                    }

                    if (touched)
                    {
                        definition.CollectionSecondaryKeys = secondaryKeys;
                    }
                }
            }

            if (definition.CollectionPrimaryKey != null && definition.CollectionPrimaryKey.StartsWith(Constants.Certificates.Prefix))
            {
                touched = true;
                definition.CollectionPrimaryKey = definition.CollectionPrimaryKey.Substring(Constants.Certificates.Prefix.Length);
            }
        }
    }
}
