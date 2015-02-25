// -----------------------------------------------------------------------
//  <copyright file="NoOpStateMachine.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Threading;

using Rachis.Commands;
using Rachis.Interfaces;
using Rachis.Messages;

namespace Raven.Database.Bundles.Raft.Storage
{
	public class InMemoryStateMachine : IRaftStateMachine
	{
		private long lastAppliedIndex;

		public long LastAppliedIndex
		{
			get { return lastAppliedIndex; }
			private set { Thread.VolatileWrite(ref lastAppliedIndex, value); }
		}

		public void Apply(LogEntry entry, Command cmd)
		{
			LastAppliedIndex = cmd.AssignedIndex;
		}

		public bool SupportSnapshots
		{
			get
			{
				return false;
			}
		}

		public void CreateSnapshot(long index, long term, ManualResetEventSlim allowFurtherModifications)
		{
			throw new NotSupportedException();
		}

		public ISnapshotWriter GetSnapshotWriter()
		{
			throw new NotSupportedException();
		}

		public void ApplySnapshot(long term, long index, Stream stream)
		{
			throw new NotSupportedException();
		}

		public void Dispose()
		{
		}
	}
}