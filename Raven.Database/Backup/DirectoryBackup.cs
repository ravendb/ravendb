using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using Lucene.Net.Store;
using Directory = System.IO.Directory;

namespace Raven.Database.Backup
{
	public class DirectoryBackup
	{
		[DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
		public static extern bool CreateHardLink(string lpFileName, string lpExistingFileName, IntPtr lpSecurityAttributes);

		public event Action<string> Notify = delegate {  };

		private readonly string source;
		private readonly string destination;
		private readonly string tempPath;

		public DirectoryBackup(string source, string destination)
		{
			this.source = source;
			this.destination = destination;

			tempPath = Path.GetTempFileName();
			File.Delete(tempPath);
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