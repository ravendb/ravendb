// -----------------------------------------------------------------------
//  <copyright file="SynchronizationActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Logging;

namespace Raven.Database.FileSystem.Actions
{
	public class SynchronizationActions : ActionsBase
	{
		public SynchronizationActions(RavenFileSystem fileSystem, ILog log)
			: base(fileSystem, log)
		{
		}

		public void StartSynchronizeDestinationsInBackground()
		{
			Task.Factory.StartNew(async () => await SynchronizationTask.SynchronizeDestinationsAsync(), CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
		}
	}
}