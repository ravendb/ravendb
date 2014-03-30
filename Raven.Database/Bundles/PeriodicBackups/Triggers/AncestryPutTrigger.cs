// -----------------------------------------------------------------------
//  <copyright file="AncestryPutTrigger.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition;
using Raven.Abstractions.Data;
using Raven.Database.Plugins;

namespace Raven.Database.Bundles.PeriodicBackups.Triggers
{
    [ExportMetadata("Bundle", "PeriodicBackup")]
    [ExportMetadata("Order", 10001)]
    [InheritedExport(typeof(AbstractPutTrigger))]
    public class AncestryPutTrigger : AbstractPutTrigger
    {
        public override void OnPut(string key, Raven.Json.Linq.RavenJObject document, Raven.Json.Linq.RavenJObject metadata, Abstractions.Data.TransactionInformation transactionInformation)
        {
            using (Database.DisableAllTriggersForCurrentThread())
            {
                Database.TransactionalStorage.Batch(accessor =>
                {
                    var tombstone = accessor.Lists.Read(Constants.RavenPeriodicBackupsDocsTombstones, key);
                    if (tombstone == null)
                        return;
                    accessor.Lists.Remove(Constants.RavenPeriodicBackupsDocsTombstones, key);
                });
            }
        }
    }
}