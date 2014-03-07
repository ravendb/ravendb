// -----------------------------------------------------------------------
//  <copyright file="IdGenerator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;

using Raven.Database.Server.RavenFS.Storage.Voron.Impl;

using Voron;
using Voron.Impl;

namespace Raven.Database.Server.RavenFS.Storage.Voron
{
    public class IdGenerator
    {
        private readonly ConcurrentDictionary<string, int> tableIds;

        public IdGenerator(TableStorage storage)
        {
            tableIds = new ConcurrentDictionary<string, int>();

            using (var snapshot = storage.CreateSnapshot())
            {
                var pages = storage.Pages.TableName;
                tableIds.TryAdd(pages, ReadLastIdFromTable(storage.Pages, snapshot));

                var usage = storage.Usage.TableName;
                tableIds.TryAdd(usage, ReadLastIdFromTable(storage.Usage, snapshot));

                var signatures = storage.Signatures.TableName;
                tableIds.TryAdd(signatures, ReadLastIdFromTable(storage.Signatures, snapshot));
            }
        }

        public int GetNextIdForTable(Table table)
        {
            if (!tableIds.ContainsKey(table.TableName))
                throw new InvalidOperationException(string.Format("Id generation has not been configured for table '{0}'.", table.TableName));

            return tableIds.AddOrUpdate(table.TableName, s =>
            {
                throw new NotSupportedException();
            }, (s, l) => l + 1);
        }

        private static int ReadLastIdFromTable(Table table, SnapshotReader snapshot)
        {
            using (var iterator = table.Iterate(snapshot, null))
            {
                if (!iterator.Seek(Slice.AfterAllKeys))
                    return 0;

                var id = iterator.CurrentKey.ToString();
                return int.Parse(id);
            }
        }
    }
}