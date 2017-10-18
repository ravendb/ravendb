using System.Collections.Generic;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using static Raven.Server.Documents.DocumentsStorage;

namespace Raven.Server.Storage.Schema.Updates.Documents
{
    public class From10 : ISchemaUpdate
    {
        public bool Update(Transaction readTx, Transaction writeTx, ConfigurationStorage configurationStorage, DocumentsStorage documentsStorage)
        {
            // Update collections
            using (documentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var table = writeTx.OpenTable(CollectionsSchema, CollectionsSlice);
                var collections = new List<string>();
                foreach (var tvr in table.SeekByPrimaryKey(Slices.BeforeAllKeys, 0))
                {
                    var collection = TableValueToString(context, (int)CollectionsTable.Name, ref tvr.Reader);
                    collections.Add(collection);
                    table.Delete(tvr.Reader.Id);
                }
                foreach (var collection in collections)
                {
                    using (DocumentIdWorker.GetStringPreserveCase(context, collection, out Slice collectionSlice))
                    using (table.Allocate(out TableValueBuilder tvr))
                    {
                        tvr.Add(collectionSlice);
                        table.Insert(tvr);
                    }
                }
            }

            return true;
        }
    }
}
