using System.Collections.Generic;
using Raven.Server.ServerWide;
using Voron;

namespace Raven.Server.Storage.Schema
{
    public sealed class SchemaUpgradeExtensions
    {
        public const string DbKey = "db/";

        public static List<string> GetDatabases(UpdateStep step)
        {
            var dbs = new List<string>();

            using (var items = step.ReadTx.OpenTable(ClusterStateMachine.ItemsSchema, ClusterStateMachine.Items))
            using (Slice.From(step.ReadTx.Allocator, DbKey, out var loweredPrefix))
            {
                foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, 0))
                {
                    dbs.Add(ClusterStateMachine.GetCurrentItemKey(result.Value).Substring(DbKey.Length));
                }
            }

            return dbs;
        }
    }
}
