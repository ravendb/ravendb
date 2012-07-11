//-----------------------------------------------------------------------
// <copyright file="MemoryPersistentSource.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace Raven.Munin
{
	public class MemoryPersistentSource : AbstractPersistentSource
	{
		private MemoryStream log;

		public MemoryPersistentSource()
		{
			log = new MemoryStream();
			CreatedNew = true;
		}

		protected override Stream CreateClonedStreamForReadOnlyPurposes()
		{
			var memoryStream = log;
			var buffer = memoryStream.GetBuffer();
			return new MemoryStream(buffer, 0, buffer.Length, false);
		}

		public MemoryPersistentSource(byte[] data)
		{
			log = new MemoryStream(data);
			CreatedNew = true;
		}

		protected override Stream Log { get { return log; } }

		#region IPersistentSource Members

		public override void ReplaceAtomically(Stream newLog)
		{
			this.log = (MemoryStream)newLog;
		}

		public override Stream CreateTemporaryStream()
		{
			return new MemoryStream();
		}

		public override void FlushLog()
		{
		}

		public override RemoteManagedStorageState CreateRemoteAppDomainState()
		{
			return new RemoteManagedStorageState
			{
				Log = ((MemoryStream)Log).ToArray(),
			};
		}

		public override void EnsureCapacity(int value)
		{
			log.Capacity = value;
		}

		#endregion
	}
}