//-----------------------------------------------------------------------
// <copyright file="DirectoryBackup.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Directory = System.IO.Directory;
using Raven.Database.Extensions;

namespace Raven.Database.Backup
{
	public class DirectoryBackup
	{
		[DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
		public static extern bool CreateHardLink(string lpFileName, string lpExistingFileName, IntPtr lpSecurityAttributes);

		[DllImport("kernel32.dll", CharSet = CharSet.Unicode)]
		public static extern bool MoveFileEx(string lpExistingFileName, string lpNewFileName, int dwFlags);

		public const int MoveFileDelayUntilReboot = 0x4;

		public event Action<string, BackupStatus.BackupMessageSeverity> Notify = delegate { };

		private readonly Dictionary<string, long> fileToSize = new Dictionary<string, long>();
		private static readonly ILog logger = LogManager.GetCurrentClassLogger();

		private readonly string source;
		private readonly string destination;
		private readonly string tempPath;
		private readonly bool allowOverwrite;

		public DirectoryBackup(string source, string destination, string tempPath, bool allowOverwrite)
		{
			this.source = source;
			this.destination = destination;
			this.tempPath = tempPath;
			this.allowOverwrite = allowOverwrite;

			if (Directory.Exists(tempPath) == false)
				Directory.CreateDirectory(tempPath);
			
			if (!allowOverwrite && Directory.Exists(destination))
				throw new InvalidOperationException("Directory exists and overwrite was not explicitly requested by user: " + destination);

			if (Directory.Exists(destination) == false)
				Directory.CreateDirectory(destination);
		}

		/// <summary>
		/// The process for backing up a directory index is simple:
		/// a) create hard links to all the files in the lucene directory in a temp director
		///	   that gives us the current snapshot, and protect us from lucene's
		///    deleting files.
		/// b) copy the hard links to the destination directory
		/// c) delete the temp directory
		/// </summary>
		public void Execute()
		{
			if (allowOverwrite) // clean destination folder; we want to do this as close as possible to the actual backup operation
			{
				IOExtensions.DeleteDirectory(destination);
				Directory.CreateDirectory(destination);
			}

			foreach (var file in Directory.EnumerateFiles(tempPath))
			{
				Notify("Copying " + Path.GetFileName(file), BackupStatus.BackupMessageSeverity.Informational);
				var fullName = new FileInfo(file).FullName;
				FileCopy(file, Path.Combine(destination, Path.GetFileName(file)), fileToSize[fullName]);
				Notify("Copied " + Path.GetFileName(file), BackupStatus.BackupMessageSeverity.Informational);
			}

			try
			{
				IOExtensions.DeleteDirectory(tempPath);
			}
			catch (Exception e) //cannot delete, probably because there is a file being written there
			{
				logger.WarnException(
					string.Format("Could not delete {0}, will delete those on startup", tempPath),
					e);

				foreach (var file in Directory.EnumerateFiles(tempPath))
				{
					MoveFileEx(file, null, MoveFileDelayUntilReboot);
				}
				MoveFileEx(tempPath, null, MoveFileDelayUntilReboot);
			}
		}

		private static void FileCopy(string src, string dest, long size)
		{
			var buffer = new byte[16 * 1024];
			using (var srcStream = File.Open(src,FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
			{
				if(File.Exists(dest))
					File.SetAttributes(dest,FileAttributes.Normal);
				using (var destStream = File.Create(dest, buffer.Length))
				{
					while (true)
					{
						var read = srcStream.Read(buffer, 0, (int)Math.Min(buffer.Length, size));
						if (read == 0)
							break;
						size -= read;
						destStream.Write(buffer, 0, read);
					}
					destStream.Flush();
				}
			}
		}

		public void Prepare()
		{
			string[] sourceFilesSnapshot;
			try
			{
				sourceFilesSnapshot = Directory.GetFiles(source);
			}
			catch (Exception e)
			{
				logger.WarnException("Could not get directory files, maybe it was deleted", e);
				return;
			}
			for (int index = 0; index < sourceFilesSnapshot.Length; index++)
			{
				var sourceFile = sourceFilesSnapshot[index];
				if (Path.GetFileName(sourceFile) == "write.lock")
					continue; // skip the Lucene lock file

				var destFileName = Path.Combine(tempPath, Path.GetFileName(sourceFile));
				var success = CreateHardLink(destFileName, sourceFile, IntPtr.Zero);

				if (success == false)
				{
					// 'The system cannot find the file specified' is explicitly ignored here
					if (Marshal.GetLastWin32Error() != 0x80004005)
						throw new Win32Exception();
					sourceFilesSnapshot[index] = null;
					continue;
				}

				try
				{
					var fileInfo = new FileInfo(destFileName);
					fileToSize[fileInfo.FullName] = fileInfo.Length;
				}
				catch (IOException)
				{
					sourceFilesSnapshot[index] = null;
					// something happened to this file, probably was removed somehow
				}
			}

			// we have to do this outside the main loop because we mustn't
			// do any modification to the DB until we capture the current sizes
			// of all the files
			foreach (var sourceFile in sourceFilesSnapshot)
			{
				if(sourceFile == null)
					continue;
				Notify("Hard linked " + sourceFile, BackupStatus.BackupMessageSeverity.Informational);
			}
		}
	}
}
