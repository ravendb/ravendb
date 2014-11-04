// -----------------------------------------------------------------------
//  <copyright file="CheckFreeDiskSpace.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Server;

namespace Raven.Database.Plugins.Builtins
{
	public class CheckFreeDiskSpace : IServerStartupTask, IDisposable
	{
		[DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
		[return: MarshalAs(UnmanagedType.Bool)]
		static extern bool GetDiskFreeSpaceEx(string lpDirectoryName,
		   out ulong lpFreeBytesAvailable,
		   out ulong lpTotalNumberOfBytes,
		   out ulong lpTotalNumberOfFreeBytes);

		private Timer checkTimer;
		private RavenDbServer server;

		const double FreeThreshold = 0.15;

		public void Execute(RavenDbServer server)
		{
			this.server = server;
			checkTimer = new Timer(ExecuteCheck, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(30));
		}

		private void ExecuteCheck(object state)
		{
			if (server.Disposed)
			{
				Dispose();
				return;
			}

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
				pathsToCheck.Add(filesystem.Configuration.JournalsStoragePath);
			});

			var roots = pathsToCheck.Where(path => path != null && Path.IsPathRooted(path) && path.StartsWith("\\\\") == false).Select(Path.GetPathRoot).ToList();
			var uniqueRoots = new HashSet<string>(roots);

			var unc = pathsToCheck.Where(path => path != null && path.StartsWith("\\\\")).ToList();
			var uniqueUncRoots = new HashSet<string>(unc.Select(Path.GetPathRoot));

			var lacksFreeSpace = new List<string>();

			foreach (var drive in DriveInfo.GetDrives())
			{
				if (uniqueRoots.Contains(drive.Name) == false)
					continue;

				if (drive.TotalFreeSpace * 1.0 / drive.TotalSize < FreeThreshold)
					lacksFreeSpace.Add(drive.Name);
			}

			foreach (var uncRoot in uniqueUncRoots)
			{
				ulong freeBytesAvailable;
				ulong totalNumberOfBytes;
				ulong totalNumberOfFreeBytes;

				var success = GetDiskFreeSpaceEx(uncRoot, out freeBytesAvailable, out totalNumberOfBytes, out totalNumberOfFreeBytes);

				if (success && freeBytesAvailable * 1.0 / totalNumberOfBytes < FreeThreshold)
					lacksFreeSpace.Add(uncRoot);
			}

			if (lacksFreeSpace.Any())
			{
				server.SystemDatabase.AddAlert(new Alert
				{
					AlertLevel = AlertLevel.Warning,
					CreatedAt = SystemTime.UtcNow,
					Title = string.Format("Database disk{0} ({1}) has less than {2}% free space.", lacksFreeSpace.Count() > 1 ? "s" : string.Empty, string.Join(", ", lacksFreeSpace), (int)(FreeThreshold * 100)),
					UniqueKey = "Free space"
				});
			}
		}

		public void Dispose()
		{
			if (checkTimer != null)
				checkTimer.Dispose();
		}
	}
}