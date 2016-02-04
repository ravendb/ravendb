// -----------------------------------------------------------------------
//  <copyright file="PurgeTombstones.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Plugins.Builtins
{
    public class PurgeOutdatedTombstones : IStartupTask
    {
        public void Execute(DocumentDatabase database)
        {
            database.Maintenance.PurgeOutdatedTombstones();
        }
    }
}
