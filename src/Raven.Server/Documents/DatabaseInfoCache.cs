using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Client.Data;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents
{
    public class DatabaseInfoCache
    {

        protected readonly Logger Logger;

        private StorageEnvironment _environment;

        private TransactionContextPool _contextPool;

        private readonly TableSchema _databaseInfoSchema = new TableSchema();

        public DatabaseInfoCache()
        {
            Logger = LoggingSource.Instance.GetLogger<DatabaseInfoCache>("DatabaseInfoCache");
            _databaseInfoSchema.DefineKey(new TableSchema.SchemaIndexDef
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
                _databaseInfoSchema.Create(tx, DatabaseInfoSchema.DatabaseInfoTree);

                tx.Commit();
            }
        }

        public unsafe void InsertDatabaseInfo(DynamicJsonValue databaseInfo, string databaseName)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(_databaseInfoSchema, DatabaseInfoSchema.DatabaseInfoTree);


                using (var id = context.GetLazyString(databaseName.ToLowerInvariant()))
                using (
                    var json = context.ReadObject(databaseInfo, "DatabaseInfo",
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

        public bool WriteDatabaseInfo(BlittableJsonTextWriter writer, string databaseName)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenReadTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(_databaseInfoSchema, DatabaseInfoSchema.DatabaseInfoTree);

                Slice databaseNameAsSlice;
                TableValueReader infoTvr;
                using (Slice.From(tx.InnerTransaction.Allocator, databaseName.ToLowerInvariant(), out databaseNameAsSlice))
                {
                    infoTvr = table.ReadByKey(databaseNameAsSlice);
                }
                //It seems like the database was shutdown rudly and never wrote it stats onto the disk
                if (infoTvr == null)
                    return false;
                var databaseInfoJson = Read(context, infoTvr);
                writer.WriteObject(databaseInfoJson);
                return true;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe BlittableJsonReaderObject Read(JsonOperationContext context, TableValueReader reader)
        {
            int size;
            var ptr = reader.Read(DatabaseInfoSchema.DatabaseInfoTable.JsonIndex, out size);
            return new BlittableJsonReaderObject(ptr, size, context);
        }

        /// <summary>
        /// This method deletes the database info from the cache when the databse is deleted.
        /// It assumes that the ctx already opened a write transaction.
        /// </summary>
        /// <param name="ctx">A context allocated outside the method with an open write transaction</param>
        /// <param name="databaseName">The database name as a slice</param>
        public void DeleteInternal(TransactionOperationContext ctx, Slice databaseName)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Deleteing database info for '{databaseName}'.");
            var table = ctx.Transaction.InnerTransaction.OpenTable(_databaseInfoSchema, DatabaseInfoSchema.DatabaseInfoTree);
            table.DeleteByKey(databaseName);            
        }

        public void Delete(string databaseName)
        {
            TransactionOperationContext ctx;
            Slice key;
            using (_contextPool.AllocateOperationContext(out ctx))
            using(ctx.OpenWriteTransaction())
            using (Slice.From(ctx.Allocator, databaseName.ToLowerInvariant(), out key))
            {
                DeleteInternal(ctx, key);
            }
        }

        public static class DatabaseInfoSchema
        {
            public const string DatabaseInfoTree = "DatabaseInfo";

            public static class DatabaseInfoTable
            {
#pragma warning disable 169
                public const int IdIndex = 0;
                public const int JsonIndex = 1;
#pragma warning restore 169
            }
        }


    }
}
