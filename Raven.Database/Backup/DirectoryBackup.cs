using System;
using System.Collections.Generic;
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

        public event Action<string> Notify = delegate { };

        private Dictionary<string, long> fileToSize = new Dictionary<string, long>();
        private readonly ILog logger = LogManager.GetLogger(typeof(DirectoryBackup));

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
                if (Path.GetFileName(file) == "write.lock")
                    continue; // skip the Lucne lock file

                Notify("Copying " + Path.GetFileName(file));
                var fullName = new FileInfo(file).FullName;
                FileCopy(file, Path.Combine(destination, Path.GetFileName(file)), fileToSize[fullName]);
                Notify("Copied " + Path.GetFileName(file));
            }

            try
            {
                IOExtensions.DeleteDirectory(tempPath);
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
            var sourceFilesSnapshot = Directory.GetFiles(source);
            foreach (var sourceFile in sourceFilesSnapshot)
            {
                var destFileName = Path.Combine(tempPath, Path.GetFileName(sourceFile));
                CreateHardLink(
                    destFileName,
                    sourceFile,
                    IntPtr.Zero
                    );

                var fileInfo = new FileInfo(destFileName);
                fileToSize[fileInfo.FullName] = fileInfo.Length;
            }

            // we have to do this outside the main loop because we mustn't
            // do any modification to the DB until we capture the current sizes
            // of all the files
            foreach (var sourceFile in sourceFilesSnapshot)
            {
                Notify("Hard linked " + sourceFile);
            }

        }
    }
}
