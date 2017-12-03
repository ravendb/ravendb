// -----------------------------------------------------------------------
//  <copyright file="PurgeTombstones.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;
using Raven.Abstractions.Logging;

namespace Raven.Database.Plugins.Builtins
{
    public class PurgeOutdatedTombstones : IStartupTask
    {
        private object locker = new object();
        public DocumentDatabase Database { get; private set; }
        private readonly ILog logger = LogManager.GetCurrentClassLogger();

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
            catch (Exception e)
            {
                //Probably the transaction failed, we will try again tomorrow.
                logger.Error("Daily purge of tombstone failed",
                    e);

            }
            finally
            {
                if (tryEnter)
                    Monitor.Exit(locker);
            }            
        }
    }
}
