// -----------------------------------------------------------------------
//  <copyright file="VirtualDeleteTrigger.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition;

using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.PeriodicExports.Triggers
{
	[ExportMetadata("Bundle", "PeriodicExport")]
    [ExportMetadata("Order", 10001)]
    [InheritedExport(typeof(AbstractDeleteTrigger))]
    public class VirtualDeleteTrigger : AbstractDeleteTrigger
    {
        public override void AfterDelete(string key, TransactionInformation transactionInformation)
        {
            var metadata = new RavenJObject
			{
				{Constants.RavenDeleteMarker, true},
			};

            Database.TransactionalStorage.Batch(accessor => 
                accessor.Lists.Set(Constants.RavenPeriodicExportsDocsTombstones, key, metadata, UuidType.Documents));
        }

    }
}