//-----------------------------------------------------------------------
// <copyright file="RestoreOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using Raven.Database.Extensions;

namespace Raven.Storage.Managed.Backup
{
	public class RestoreOperation
	{
		private readonly Action<string> output;
		private readonly string backupLocation;
		private readonly string databaseLocation;

		public RestoreOperation(string backupLocation, string databaseLocation, Action<string> output)
		{
			this.output = output;
			this.backupLocation = backupLocation.ToFullPath();
			this.databaseLocation = databaseLocation.ToFullPath();
		}

		public void Execute()
		{
			if ((Directory.Exists(Path.Combine(backupLocation, "Indexes")) == false) ||
				(Directory.Exists(Path.Combine(backupLocation, "IndexDefinitions")) == false))
			{
				output(backupLocation + " doesn't look like a valid backup");
				throw new InvalidOperationException(backupLocation + " doesn't look like a valid backup");
			}

			if (Directory.Exists(databaseLocation) && Directory.GetFileSystemEntries(databaseLocation).Length > 0)
			{
				output("Database already exists, cannot restore to an existing database.");
				throw new IOException("Database already exists, cannot restore to an existing database.");
			}

			if (Directory.Exists(databaseLocation) == false)
				Directory.CreateDirectory(databaseLocation);

			CopyAll(new DirectoryInfo(backupLocation), new DirectoryInfo(databaseLocation), output);
		}

		private static void CopyAll(DirectoryInfo source, DirectoryInfo target, Action<string> output)
		{
			// Check if the target directory exists, if not, create it.
			if (Directory.Exists(target.FullName) == false)
			{
				Directory.CreateDirectory(target.FullName);
			}

			// Copy each file into it's new directory.
			foreach (FileInfo fi in source.GetFiles())
			{
				output(string.Format(@"Copying {0}\{1}", target.FullName, fi.Name));
				Console.WriteLine(@"Copying {0}\{1}", target.FullName, fi.Name);
				fi.CopyTo(Path.Combine(target.ToString(), fi.Name), true);
			}

			// Copy each subdirectory using recursion.
			foreach (DirectoryInfo diSourceSubDir in source.GetDirectories())
			{
				DirectoryInfo nextTargetSubDir =
					target.CreateSubdirectory(diSourceSubDir.Name);
				CopyAll(diSourceSubDir, nextTargetSubDir, output);
			}
		}

	}
}