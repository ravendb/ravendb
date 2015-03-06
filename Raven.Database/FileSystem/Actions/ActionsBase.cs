// -----------------------------------------------------------------------
//  <copyright file="ActionsBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util.Streams;
using Raven.Database.FileSystem.Infrastructure;
using Raven.Database.FileSystem.Notifications;
using Raven.Database.FileSystem.Search;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Synchronization;
using Raven.Database.FileSystem.Synchronization.Rdc.Wrapper;

namespace Raven.Database.FileSystem.Actions
{
	public abstract class ActionsBase
	{
		protected RavenFileSystem FileSystem { get; private set; }

		protected NotificationPublisher Publisher
		{
			get { return FileSystem.Publisher; }
		}

		protected BufferPool BufferPool
		{
			get { return FileSystem.BufferPool; }
		}

		protected SigGenerator SigGenerator
		{
			get { return FileSystem.SigGenerator; }
		}

		protected Historian Historian
		{
			get { return FileSystem.Historian; }
		}

		protected SynchronizationTask SynchronizationTask
		{
			get { return FileSystem.SynchronizationTask; }
		}

		protected FileLockManager FileLockManager
		{
			get { return FileSystem.FileLockManager; }
		}

		protected ITransactionalStorage Storage
		{
			get { return FileSystem.Storage; }
		}

		protected IndexStorage Search
		{
			get { return FileSystem.Search; }
		}

		protected ILog Log { get; private set; }

		protected ActionsBase(RavenFileSystem fileSystem, ILog log)
		{
			FileSystem = fileSystem;
			Log = log;
		}
	}
}