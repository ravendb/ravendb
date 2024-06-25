using System;
using Voron;
using Voron.Data;
using Voron.Data.Tables;
using Voron.Exceptions;

namespace Raven.Server.Storage.Schema.Updates.Documents
{
    public sealed unsafe class From40013 : ISchemaUpdate
    {
        public int From => 40_013;
        public int To => 40_014;
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Documents;

        public bool Update(UpdateStep step)
        {
            try
            {
                var tx = step.ReadTx;
                using (var it = tx.LowLevelTransaction.RootObjects.Iterate(false))
                {
                    if (it.Seek(Slices.BeforeAllKeys) == false)
                        return true;

                    var stats = new TableSchemaStatsReference();

                    do
                    {
                        var rootObjectType = tx.GetRootObjectType(it.CurrentKey);
                        if (rootObjectType == RootObjectType.FixedSizeTree)
                        {
                            tx.FixedTreeFor(it.CurrentKey).ValidateTree_Forced();
                        } else if (rootObjectType == RootObjectType.Table)
                        {
                            var tableTree = tx.ReadTree(it.CurrentKey, RootObjectType.Table);
                            var writtenSchemaData = tableTree.DirectRead(TableSchema.SchemasSlice);
                            var writtenSchemaDataSize = tableTree.GetDataSize(TableSchema.SchemasSlice);
                            var schema = TableSchema.ReadFrom(tx.Allocator, writtenSchemaData, writtenSchemaDataSize);
                            new Table(schema, it.CurrentKey, tx, tableTree, stats, schema.TableType).AssertValidFixedSizeTrees();
                        }
                    } while (it.MoveNext());
                }

                return true;
            }
            catch (Exception e)
            {
                VoronUnrecoverableErrorException.Raise("Failed to update documents from version 13 to 14, due to an internal error. " +
                                                       "Use the Voron.Recovery tool to export this database and import it into a new database.", e);
            }

            return false;
        }
    }
}
