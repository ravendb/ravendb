// -----------------------------------------------------------------------
//  <copyright file="PurgeTombstones.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Database.Actions;

namespace Raven.Database.Plugins.Builtins
{
    public class PurgeOutdatedTombstones : IStartupTask
    {
        public void Execute(DocumentDatabase database)
        {
            long id;
            var task = Task.Run(() => database.Maintenance.PurgeOutdatedTombstones());
            database.Tasks.AddTask(task, new TaskBasedOperationState(task), 
                                    new TaskActions.PendingTaskDescription
                                    {
                                        TaskType = TaskActions.PendingTaskType.PurgeTombstones,
                                        Description = "Startup Task - Purge Outdated Tombstones (if relevant)",
                                        StartTime = SystemTime.UtcNow
                                    }, out id);
        }
    }
}
