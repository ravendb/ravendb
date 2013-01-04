//-----------------------------------------------------------------------
// <copyright file="CleanupOldDynamicIndexes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Database.Indexing;
using Raven.Database.Queries;

namespace Raven.Database.Plugins.Builtins
{
	public class CleanupOldDynamicIndexes : IStartupTask, IRepeatedAction
	{
		private DocumentDatabase database;

		public void Execute(DocumentDatabase theDatabase)
		{
			if (theDatabase == null) 
				throw new ArgumentNullException("theDatabase");
			
			database = theDatabase;

			BackgroundTaskExecuter.Instance.Repeat(this);
		}

		public TimeSpan RepeatDuration
		{
			get { return database.Configuration.TempIndexCleanupPeriod; }
		}

		public bool IsValid
		{
			get { return database.Disposed == false; }
		}

		public void Execute()
		{
			var dynamicQueryRunner = database.ExtensionsState.Values.OfType<DynamicQueryRunner>().FirstOrDefault();
			if (dynamicQueryRunner == null)
				return;

			dynamicQueryRunner.CleanupCache();
		}
	}
}
