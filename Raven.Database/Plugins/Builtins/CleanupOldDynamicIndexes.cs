using System;

namespace Raven.Database.Plugins.Builtins
{
    public class CleanupOldDynamicIndexes : IStartupTask
    {
        public void Execute(DocumentDatabase database)
        {
            while(database.WorkContext.DoWork)
            {
                database.DynamicQueryRunner.CleanupCache();

                database.WorkContext.WaitForWork(TimeSpan.FromMinutes(1));
            }
        }
    }
}