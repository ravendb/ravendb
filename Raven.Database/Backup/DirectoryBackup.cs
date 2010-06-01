using System;
using System.IO;
using System.Runtime.InteropServices;
using log4net;
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

		public event Action<string> Notify = delegate {  };

		private ILog logger = LogManager.GetLogger(typeof(DirectoryBackup));

		private readonly string source;
		private readonly string destination;
		private readonly string tempPath;

		public DirectoryBackup(string source, string destination, string tempPath)
		{
			this.source = source;
			this.destination = destination;
			this.tempPath = tempPath;

			if (Directory.Exists(tempPath) == false)
				Directory.CreateDirectory(tempPath);
			if (Directory.Exists(destination) == false)
				Directory.CreateDirectory(destination);
		}

		/// <summary>
		/// The process for backing up a directory index is simple:
		/// a) create hard links to all the files in the lucene directory in a temp director
		///	   that gives us the current snapshop, and protect us from lucene's
		///    deleting files.
		/// b) copy the hard links to the destination directory
		/// c) delete the temp directory
		/// </summary>
		public void Execute()
		{
			foreach (var file in Directory.EnumerateFiles(tempPath))
			{
				Notify("Copying " + Path.GetFileName(file)); 
				File.Copy(file, Path.Combine(destination, Path.GetFileName(file)), overwrite: true);
				Notify("Copied " + Path.GetFileName(file));
			}

			try
			{
				Directory.Delete(tempPath, true);
			}
			catch (Exception e) //cannot delete, probably because there is a file being written there
			{
				logger.WarnFormat(e, "Could not delete {0}, will delete those on startup", tempPath);

				foreach (var file in Directory.EnumerateFiles(tempPath))
				{
					MoveFileEx(file, null, MoveFileDelayUntilReboot);
				}
				MoveFileEx(tempPath, null, MoveFileDelayUntilReboot);
			}
		}

		public void Prepare()
		{
			var sourceFilesSnapshot = Directory.GetFiles(source);
			foreach (var sourceFile in sourceFilesSnapshot)
			{
				Notify("Hard linking " + sourceFile);
				CreateHardLink(
					Path.Combine(tempPath, Path.GetFileName(sourceFile)),
					sourceFile,
					IntPtr.Zero
					);
			}
		}
	}
}