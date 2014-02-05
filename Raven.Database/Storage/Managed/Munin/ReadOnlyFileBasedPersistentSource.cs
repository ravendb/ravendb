//-----------------------------------------------------------------------
// <copyright file="ReadOnlyFileBasedPersistentSource.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;

namespace Raven.Munin
{
	/// <summary>
	/// Simple read only version of the file based data.
	/// It is mostly meant for read only access from remote application domain.
	/// </summary>
	public class ReadOnlyFileBasedPersistentSource : AbstractPersistentSource
	{
		private readonly string basePath;
		private readonly string prefix;
		private readonly string logPath;

		private Stream log;

		public ReadOnlyFileBasedPersistentSource(string basePath, string prefix)
		{
			this.basePath = basePath;
			this.prefix = prefix;
			logPath = Path.Combine(basePath, prefix + ".ravendb");

			OpenFiles();
		}

		private void OpenFiles()
		{
			log = CreateClonedStreamForReadOnlyPurposes();
		}

		#region IPersistentSource Members

		protected override Stream CreateClonedStreamForReadOnlyPurposes()
		{
			return new FileStream(logPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 4096, FileOptions.SequentialScan);
		}

		protected override Stream Log
		{
			get { return log; }
		}

		public override void ReplaceAtomically(Stream newNewLog)
		{
		   throw new NotSupportedException();
		}

		public override Stream CreateTemporaryStream()
		{
			throw new NotSupportedException();
		}

		public override void FlushLog()
		{
			throw new NotSupportedException();
		}

		public override RemoteManagedStorageState CreateRemoteAppDomainState()
		{
			return new RemoteManagedStorageState
			{
				Path = basePath,
				Prefix = prefix
			};
		}

		#endregion

		public override void EnsureCapacity(int value)
		{
			// nothing to do here
		}

		public override void Dispose()
		{
			log.Dispose();
			base.Dispose();
		}
	}
}