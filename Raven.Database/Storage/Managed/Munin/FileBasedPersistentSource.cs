//-----------------------------------------------------------------------
// <copyright file="FileBasedPersistentSource.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;

namespace Raven.Munin
{
	public class FileBasedPersistentSource : AbstractPersistentSource
	{
		private readonly string basePath;
		private readonly string logPath;
		private readonly string prefix;
		private readonly bool writeThrough;

		private FileStream log;

		public FileBasedPersistentSource(string basePath, string prefix, bool writeThrough)
		{
			this.basePath = basePath;
			this.prefix = prefix;
			this.writeThrough = writeThrough;
			logPath = Path.Combine(basePath, prefix + ".ravendb");

			RecoverFromFailedRename(logPath);

			CreatedNew = File.Exists(logPath) == false;

			OpenFiles();
		}

		protected override Stream Log
		{
			get { return log; }
		}

		private void OpenFiles()
		{
			log = new FileStream(logPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, 4096,
								 writeThrough
									 ? FileOptions.WriteThrough | FileOptions.SequentialScan
									 : FileOptions.SequentialScan
				);
		}

		protected override Stream CreateClonedStreamForReadOnlyPurposes()
		{
			return new FileStream(logPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
		}

		public override void ReplaceAtomically(Stream newNewLog)
		{
			var newLogStream = ((FileStream) newNewLog);
			string logTempName = newLogStream.Name;
			newLogStream.Flush();
			newLogStream.Dispose();

			newNewLog.Dispose();

			log.Dispose();

			string renamedLogFile = logPath + ".rename_op";

			File.Move(logPath, renamedLogFile);

			File.Move(logTempName, logPath);

			File.Delete(renamedLogFile);

			OpenFiles();
		}

		public override Stream CreateTemporaryStream()
		{
			string tempFile = Path.Combine(basePath, Path.GetFileName(Path.GetTempFileName()));
			return File.Open(tempFile, FileMode.Create, FileAccess.ReadWrite);
		}

		public override void FlushLog()
		{
			log.Flush(writeThrough);
		}

		public override RemoteManagedStorageState CreateRemoteAppDomainState()
		{
			return new RemoteManagedStorageState
			{
				Path = basePath,
				Prefix = prefix
			};
		}

		private static void RecoverFromFailedRename(string file)
		{
			string renamedFile = file + ".rename_op";
			if (File.Exists(renamedFile) == false) // not in the middle of rename op, we are good
				return;

			if (File.Exists(file))
				// we successfully renamed the new file and crashed before we could remove the old copy
			{
				//just complete the op and we are good (committed)
				File.Delete(renamedFile);
			}
			else // we successfully renamed the old file and crashed before we could remove the new file
			{
				// just undo the op and we are good (rollback)
				File.Move(renamedFile, file);
			}
		}

		public override void EnsureCapacity(int value)
		{
			// not sure how we can reserve space on the file system
		}

		public override void Dispose()
		{
			Action parentDispose = base.Dispose;
			Write(_ => log.Dispose());
			parentDispose();

		}

		public void Delete()
		{
			File.Delete(logPath);
		}
	}
}