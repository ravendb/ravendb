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

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Server;

namespace Raven.Database.Plugins.Builtins
{
	public class CheckFreeDiskSpace : IServerStartupTask
	{
		[DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
		[return: MarshalAs(UnmanagedType.Bool)]
		static extern bool GetDiskFreeSpaceEx(string lpDirectoryName,
		   out ulong lpFreeBytesAvailable,
		   out ulong lpTotalNumberOfBytes,
		   out ulong lpTotalNumberOfFreeBytes);

		private RavenDbServer server;

		const double FreeThreshold = 0.15;

		public void Execute(RavenDbServer ravenDbServer)
		{
			server = ravenDbServer;
			server.SystemDatabase.TimerManager.NewTimer(ExecuteCheck, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(30));
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
				pathsToCheck.Add(database.Configuration.Storage.Esent.JournalsStoragePath);
				pathsToCheck.Add(database.Configuration.Storage.Voron.JournalsStoragePath);
				pathsToCheck.Add(database.Configuration.DataDirectory);
			});

			server.Options.FileSystemLandlord.ForAllFileSystems(filesystem =>
			{
				pathsToCheck.Add(filesystem.Configuration.FileSystem.DataDirectory);
				pathsToCheck.Add(filesystem.Configuration.FileSystem.IndexStoragePath);
				pathsToCheck.Add(filesystem.Configuration.Storage.Esent.JournalsStoragePath);
				pathsToCheck.Add(filesystem.Configuration.Storage.Voron.JournalsStoragePath);
			});

			var roots = pathsToCheck.Where(path => path != null && Path.IsPathRooted(path) && path.StartsWith("\\\\") == false).Select(Path.GetPathRoot).ToList();
			var uniqueRoots = new HashSet<string>(roots);

			var unc = pathsToCheck.Where(path => path != null && path.StartsWith("\\\\")).ToList();
			var uniqueUncRoots = new HashSet<string>(unc.Select(Path.GetPathRoot));

			var lacksFreeSpace = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

			var driveInfos = DriveInfo.GetDrives();
			foreach (var root in uniqueRoots)
			{
				var result = DiskSpaceChecker.GetFreeDiskSpace(root, driveInfos);
				if (result == null)
					continue;

				if (result.TotalFreeSpaceInBytes * 1.0 / result.TotalSize < FreeThreshold)
					lacksFreeSpace.Add(result.DriveName);
			}

			foreach (var uncRoot in uniqueUncRoots)
			{
				var result = DiskSpaceChecker.GetFreeDiskSpace(uncRoot, null);
				if (result == null)
					continue;

				if (result.TotalFreeSpaceInBytes * 1.0 / result.TotalSize < FreeThreshold)
					lacksFreeSpace.Add(result.DriveName);
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
		}

		public static class DiskSpaceChecker
		{
			public static DiskSpaceResult GetFreeDiskSpace(string pathToCheck, DriveInfo[] driveInfo)
			{
				if (string.IsNullOrEmpty(pathToCheck))
					return null;

				if (Path.IsPathRooted(pathToCheck) && pathToCheck.StartsWith("\\\\") == false)
				{
					var root = Path.GetPathRoot(pathToCheck);

					foreach (var drive in driveInfo)
					{
						if (root.Contains(drive.Name) == false)
							continue;

						return new DiskSpaceResult
						{
							DriveName = root,
							TotalFreeSpaceInBytes = drive.TotalFreeSpace,
							TotalSize = drive.TotalSize
						};
					}

					return null;
				}

				if (pathToCheck.StartsWith("\\\\"))
				{
					var uncRoot = Path.GetPathRoot(pathToCheck);

					ulong freeBytesAvailable;
					ulong totalNumberOfBytes;
					ulong totalNumberOfFreeBytes;
					var success = GetDiskFreeSpaceEx(uncRoot, out freeBytesAvailable, out totalNumberOfBytes, out totalNumberOfFreeBytes);

					if (success == false)
						return null;

					return new DiskSpaceResult
						   {
							   DriveName = uncRoot,
							   TotalFreeSpaceInBytes = (long)freeBytesAvailable,
							   TotalSize = (long)totalNumberOfBytes
						   };
				}

				return null;
			}

			public class DiskSpaceResult
			{
				public string DriveName { get; set; }

				public long TotalFreeSpaceInBytes { get; set; }

				public long TotalSize { get; set; }
			}
		}
	}
}