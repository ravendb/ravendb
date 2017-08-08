using System;
using System.Runtime.CompilerServices;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Commercial
{
    public class LicenseStorage
    {
        private StorageEnvironment _environment;

        private TransactionContextPool _contextPool;

        private readonly TableSchema _licenseStorageSchema = new TableSchema();
        private const string FirstServerStartDateKey = "FirstServerStartDate";
        private const string LicenseStoargeKey = "LicenseStoargeKey";

        public LicenseStorage()
        {
            _licenseStorageSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1
            });
        }

        public void Initialize(StorageEnvironment environment, TransactionContextPool contextPool)
        {
            _environment = environment;
            _contextPool = contextPool;

            using (contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = _environment.WriteTransaction(context.PersistentContext))
            {
                _licenseStorageSchema.Create(tx, LicenseInfoSchema.LicenseTree, 16);

                tx.Commit();
            }
        }

        public unsafe void SetFirstServerStartDate(DateTime date)
        {
            var firstServerStartDate = new DynamicJsonValue
            {
                [FirstServerStartDateKey] = date
            };

            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(_licenseStorageSchema, LicenseInfoSchema.LicenseTree);

                var id = context.GetLazyString(FirstServerStartDateKey);
                using (var json = context.ReadObject(firstServerStartDate, "DatabaseInfo",
                    BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    using (table.Allocate(out TableValueBuilder tvb))
                    {
                        tvb.Add(id.Buffer, id.Size);
                        tvb.Add(json.BasePointer, json.Size);

                        table.Set(tvb);
                    }
                }
                tx.Commit();
            }
        }

        public DateTime? GetFirstServerStartDate()
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenReadTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(_licenseStorageSchema, LicenseInfoSchema.LicenseTree);

                TableValueReader infoTvr;
                using (Slice.From(tx.InnerTransaction.Allocator, FirstServerStartDateKey, out Slice keyAsSlice))
                {
                    //It seems like the database was shutdown rudely and never wrote it stats onto the disk
                    if (table.ReadByKey(keyAsSlice, out infoTvr) == false)
                        return null;
                }

                using (var firstServerStartDateJson = Read(context, ref infoTvr))
                {
                    if (firstServerStartDateJson.TryGet(FirstServerStartDateKey, out DateTime result))
                        return result;
                }

                return null;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe BlittableJsonReaderObject Read(JsonOperationContext context, ref TableValueReader reader)
        {
            var ptr = reader.Read(LicenseInfoSchema.LicenseTable.JsonIndex, out int size);
            return new BlittableJsonReaderObject(ptr, size, context);
        }

        public unsafe void SaveLicense(License license)
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(_licenseStorageSchema, LicenseInfoSchema.LicenseTree);

                using (var id = context.GetLazyString(LicenseStoargeKey))
                using (var json = context.ReadObject(license.ToJson(), LicenseStoargeKey,
                    BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    using (table.Allocate(out TableValueBuilder tvb))
                    {
                        tvb.Add(id.Buffer, id.Size);
                        tvb.Add(json.BasePointer, json.Size);

                        table.Set(tvb);
                    }
                }
                tx.Commit();
            }
        }

        public License LoadLicense()
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenReadTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(_licenseStorageSchema, LicenseInfoSchema.LicenseTree);

                TableValueReader reader;
                using (Slice.From(context.Allocator, LicenseStoargeKey, out Slice key))
                {
                    if (table.ReadByKey(key, out reader) == false)
                        return null;
                }

                using (var licenseJson = Read(context, ref reader))
                {
                    return JsonDeserializationServer.License(licenseJson);
                }
            }
        }

        public static class LicenseInfoSchema
        {
            public const string LicenseTree = "LicenseInfo";

            public static class LicenseTable
            {
#pragma warning disable 169
                public const int IdIndex = 0;
                public const int JsonIndex = 1;
#pragma warning restore 169
            }
        }
    }
}