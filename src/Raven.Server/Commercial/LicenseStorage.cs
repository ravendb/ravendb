using System;
using System.Runtime.CompilerServices;
using Raven.Imports.Newtonsoft.Json;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Commercial
{
    public class LicenseStorage
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<LicenseStorage>(null);

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

            TransactionOperationContext context;
            using (contextPool.AllocateOperationContext(out context))
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

            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(_licenseStorageSchema, LicenseInfoSchema.LicenseTree);

                var id = context.GetLazyString(FirstServerStartDateKey);
                using (var json = context.ReadObject(firstServerStartDate, "DatabaseInfo",
                    BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var tvb = new TableValueBuilder
                    {
                        {id.Buffer, id.Size},
                        {json.BasePointer, json.Size}
                    };

                    table.Set(tvb);
                }
                tx.Commit();
            }
        }

        public DateTime? GetFirstServerStartDate()
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenReadTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(_licenseStorageSchema, LicenseInfoSchema.LicenseTree);

                Slice keyAsSlice;
                TableValueReader infoTvr;
                using (Slice.From(tx.InnerTransaction.Allocator, FirstServerStartDateKey, out keyAsSlice))
                {
                    //It seems like the database was shutdown rudly and never wrote it stats onto the disk
                    if (table.ReadByKey(keyAsSlice, out infoTvr) == false)
                        return null;
                }

                using (var firstServerStartDateJson = Read(context, infoTvr))
                {
                    DateTime result;
                    if (firstServerStartDateJson.TryGet(FirstServerStartDateKey, out result))
                        return result;
                }

                return null;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe BlittableJsonReaderObject Read(JsonOperationContext context, TableValueReader reader)
        {
            int size;
            var ptr = reader.Read(LicenseInfoSchema.LicenseTable.JsonIndex, out size);
            return new BlittableJsonReaderObject(ptr, size, context);
        }

        public unsafe void SaveLicense(License license)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(_licenseStorageSchema, LicenseInfoSchema.LicenseTree);

                using (var id = context.GetLazyString(LicenseStoargeKey))
                using (var json = context.ReadObject(license.ToJson(), LicenseStoargeKey,
                    BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var tvb = new TableValueBuilder
                    {
                        {id.Buffer, id.Size},
                        {json.BasePointer, json.Size}
                    };

                    table.Set(tvb);
                }
                tx.Commit();
            }
        }

        public License LoadLicense()
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenReadTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(_licenseStorageSchema, LicenseInfoSchema.LicenseTree);

                TableValueReader reader;
                Slice key;
                using (Slice.From(context.Allocator, LicenseStoargeKey, out key))
                {
                    if (table.ReadByKey(key, out reader) == false)
                        return null;
                }

                using (var licenseJson = Read(context, reader))
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