// -----------------------------------------------------------------------
//  <copyright file="PurgeTombstones.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;

namespace Raven.Database.Plugins.Builtins
{
    public class PurgeOutdatedTombstones : IStartupTask
    {
        private object locker = new object();
        public DocumentDatabase Database { get; private set; }

        public void Execute(DocumentDatabase database)
        {
            Database = database;
            //For disabling tombstone deletion on startup set 'Raven/TombstoneRetentionTime' to MaxValue (10675199.02:48:05.4775807)
            if (database.Configuration.TombstoneRetentionTime == TimeSpan.MaxValue)
                return;
            //Give the server some time to 'wakeup' before starting
            var timer = new Timer(PurgeOutdatedTombstonesCallback, null, TimeSpan.FromMinutes(1), TimeSpan.FromDays(1));
            
        }

        private void PurgeOutdatedTombstonesCallback(object state)
        {
            //For the rare case where the deletion is taking longer than a day
            var tryEnter = Monitor.TryEnter(locker);
            try
            {
                if (tryEnter == false)
                    return;
                Database.PurgeOutdatedTombstones();
            }
            finally
            {
                if (tryEnter)
                    Monitor.Exit(locker);
            }            
        }
    }
}
