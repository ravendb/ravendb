using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using Lucene.Net.Store;
using Directory = System.IO.Directory;

namespace Raven.Database.Backup
{
	public class LuceneBackup
	{

		[DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
		static extern bool CreateHardLink(string lpFileName, string lpExistingFileName, IntPtr lpSecurityAttributes);

		private readonly DateTime lastBackupUtc;
		private readonly string source;
		private readonly string destination;

		public LuceneBackup(string source, string destination, DateTime lastBackupUtc)
		{
			this.lastBackupUtc = lastBackupUtc;
			this.source = source;
			this.destination = destination;
		}

		/// <summary>
		/// The process for backing up a lucene index is simple:
		/// a) create hard links to all the files in the lucene directory in a temp director
		///	   that gives us the current snapshop, and protect us from lucene's
		///    deleting files.
		/// b) copy the hard links to the destination directory
		/// c) delete the temp directory
		/// </summary>
		public void Execute()
		{
			var sourceFilesSnapshot = Directory.GetFiles(source);
			var tempPath = Path.GetTempPath();
			File.Delete(tempPath);
			Directory.CreateDirectory(tempPath);

			if (Directory.Exists(destination) == false)
				Directory.CreateDirectory(destination);

			foreach (var sourceFile in sourceFilesSnapshot)
			{
				if(File.GetLastWriteTimeUtc(sourceFile) < lastBackupUtc)
					continue;

				CreateHardLink(
					Path.Combine(tempPath, Path.GetFileName(sourceFile)),
					sourceFile,
					IntPtr.Zero
					);
			}
			foreach (var file in Directory.EnumerateFiles(tempPath))
			{
				File.Copy(file, Path.Combine(destination, Path.GetFileName(file)), overwrite: true);
			}
		}
	}
}