// -----------------------------------------------------------------------
//  <copyright file="DatabaseSmugglerEmbeddedBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading.Tasks;

using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Abstractions.Util;

namespace Raven.Database.Smuggler.Embedded
{
    public abstract class DatabaseSmugglerEmbeddedBase
    {
        protected Task InitializeBatchSizeAsync(DocumentDatabase database, DatabaseSmugglerOptions options)
        {
            var current = options.BatchSize;
            options.BatchSize = Math.Min(current, database.Configuration.Core.MaxNumberOfItemsToProcessInSingleBatch);
            return new CompletedTask();
        }
    }
}
