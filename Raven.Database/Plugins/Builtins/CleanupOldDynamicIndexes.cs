using System;
using System.Threading;

namespace Raven.Database.Plugins.Builtins
{
    public class CleanupOldDynamicIndexes : AbstractBackgroundTask
    {
        protected override bool HandleWork()
        {
            Database.DynamicQueryRunner.CleanupCache();
            return false;
        }
    }
}