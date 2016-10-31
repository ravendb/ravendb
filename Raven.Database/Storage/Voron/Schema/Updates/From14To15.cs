// -----------------------------------------------------------------------
//  <copyright file="From13To14.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using Raven.Database.Storage.Voron.Impl;
using Voron;

namespace Raven.Database.Storage.Voron.Schema.Updates
{
    internal class From14To15 : SchemaUpdateBase
    {
        public override string FromSchemaVersion => "1.4";

        public override string ToSchemaVersion => "1.5";

        public override void Update(TableStorage tableStorage, Action<string> output)
        {
            foreach (var multiTreeName in new []
            {
                tableStorage.ReduceKeyTypes.GetIndexKey(Tables.ReduceKeyTypes.Indices.ByView)
            })
            {
                using (var tx = tableStorage.Environment.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var table = tx.ReadTree(multiTreeName);

                    if (table.State.EntriesCount == 0)
                    {
                        output($"Multi tree {multiTreeName} is empty. No need to migrate.");
                        continue;
                    }

                    output($"Migrating {multiTreeName}, with {table.State.EntriesCount:#,#;;0} entries");

                    tableStorage.Environment.CreateTree(tx, "temp_" + multiTreeName);

                    tx.Commit();
                }

                var sp = Stopwatch.StartNew();
                var lastKey = Slice.BeforeAllKeys;
                var count = 0;
                var totalCount = 0L;
                var hasMore = true;

                while (hasMore)
                {
                    hasMore = false;
                    using (var tx = tableStorage.Environment.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        var table = tx.ReadTree(multiTreeName);

                        var newTree = tx.ReadTree("temp_" + multiTreeName);

                        using (var it = table.Iterate())
                        {
                            if (it.Seek(lastKey))
                            {
                                do
                                {
                                    using (var multiValueIterator = table.MultiRead(it.CurrentKey))
                                    {
                                        if (multiValueIterator.Seek(Slice.BeforeAllKeys) == false)
                                            continue;

                                        do
                                        {
                                            newTree.MultiAdd(it.CurrentKey, multiValueIterator.CurrentKey);

                                        } while (multiValueIterator.MoveNext());
                                    }

                                    totalCount++;

                                    if (++count > 50000)
                                    {
                                        output("Migrated 50,000 records from multi tree " + multiTreeName + ", pulsing transaction");
                                        // now move to the next one and so we'll run with it next time
                                        if (it.MoveNext())
                                        {
                                            lastKey = it.CurrentKey.Clone();
                                            count = 0;
                                            hasMore = true;
                                        }
                                        break;
                                    }
                                } while (it.MoveNext());
                            }
                        }

                        tx.Commit();
                    }
                }

                using (var txw = tableStorage.Environment.NewTransaction(TransactionFlags.ReadWrite))
                {
                    tableStorage.Environment.DeleteTree(txw, multiTreeName);
                    tableStorage.Environment.RenameTree(txw, "temp_" + multiTreeName, multiTreeName);

                    txw.Commit();
                }

                output($"Finished migration {multiTreeName} total of {totalCount:#,#;;0} records in {sp.Elapsed}");
            }

            UpdateSchemaVersion(tableStorage, output);
        }
    }
}