//-----------------------------------------------------------------------
// <copyright file="CleanupOldDynamicIndexes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Threading;
using System.Linq;
using Raven.Database.Queries;

namespace Raven.Database.Plugins.Builtins
{
    public class CleanupOldDynamicIndexes : AbstractBackgroundTask
    {
        protected override bool HandleWork()
        {
            var dynamicQueryRunner = Database.ExtensionsState.Values.OfType<DynamicQueryRunner>().FirstOrDefault();
            if (dynamicQueryRunner == null)
                return false;

            dynamicQueryRunner.CleanupCache();
            return false;
        }

        protected override TimeSpan TimeoutForNextWork()
        {
            return Database.Configuration.TempIndexCleanupPeriod;
        }
    }
}
