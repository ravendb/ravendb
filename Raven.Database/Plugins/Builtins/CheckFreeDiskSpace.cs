// -----------------------------------------------------------------------
//  <copyright file="CheckFreeDiskSpace.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Server;

namespace Raven.Database.Plugins.Builtins
{
    public class CheckFreeDiskSpace : IServerStartupTask, IDisposable
    {
        private Timer checkTimer;
        private RavenDbServer server;

        const int FreeThreshold = 15;

        public void Execute(RavenDbServer server)
        {
            this.server = server;
            checkTimer = new Timer(ExecuteCheck, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(30)); 
        }

        private void ExecuteCheck(object state)
        {
            var pathsToCheck = new HashSet<string>();

            server.Options.DatabaseLandlord.ForAllDatabases(database =>
            {
                pathsToCheck.Add(database.Configuration.IndexStoragePath);
                pathsToCheck.Add(database.Configuration.JournalsStoragePath);
                pathsToCheck.Add(database.Configuration.DataDirectory);
            });

            server.Options.FileSystemLandlord.ForAllFileSystems(filesystem =>
            {
                pathsToCheck.Add(filesystem.Configuration.FileSystem.DataDirectory);
                pathsToCheck.Add(filesystem.Configuration.FileSystem.IndexStoragePath);
            });

            var roots = pathsToCheck.Where(path => path != null && Path.IsPathRooted(path) && path.StartsWith("\\\\") == false).Select(Path.GetPathRoot).ToList();
            var uniqueRoots = new HashSet<string>(roots);

            var unc = pathsToCheck.Where(path => path != null && path.StartsWith("\\\\")).ToList();
            var uniqueUnc = new HashSet<string>(unc);

            Console.WriteLine(uniqueRoots); //TODO: delete me
            Console.WriteLine(uniqueUnc); //TODO: delete me

            //TODO: verify network shares

            string[] lacksFreeSpace = DriveInfo.GetDrives()
                                               .Where(x => uniqueRoots.Contains(x.Name) && x.TotalFreeSpace*1.0/x.TotalSize < FreeThreshold/100.0)
                                               .Select(x => x.Name)
                                               .ToArray();

            if (lacksFreeSpace.Any())
            {
                server.SystemDatabase.AddAlert(new Alert
                {
                    AlertLevel = AlertLevel.Warning,
                    CreatedAt = SystemTime.UtcNow,
                    Title = string.Format("Database disk{0} ({1}) has less than {2}% free space.", lacksFreeSpace.Count() > 1 ? "s" : "", string.Join(", ", lacksFreeSpace), FreeThreshold),
                    UniqueKey = "Free space"
                });
            }
        }

        public void Dispose()
        {
            if (checkTimer != null)
            {
                checkTimer.Dispose();    
            }
        }
    }
}