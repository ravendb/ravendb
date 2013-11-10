// -----------------------------------------------------------------------
//  <copyright file="DebugTasks.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Database.Server.Abstractions;
using Raven.Database.Extensions;
using Raven.Json.Linq;

namespace Raven.Database.Server.Responders.Debugging
{
	using System.Collections.Generic;
	using System.Linq;

	using Raven.Database.Tasks;

	public class DebugTasks : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return @"^/debug/tasks"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET" }; }
		}
		public override void Respond(IHttpContext context)
		{
			IList<TaskMetadata> tasks = null;
			Database.TransactionalStorage.Batch(accessor =>
			{
				tasks = accessor.Tasks
					.GetPendingTasksForDebug()
					.ToList();
			});

			foreach (var taskMetadata in tasks)
			{
				var indexInstance = Database.IndexStorage.GetIndexInstance(taskMetadata.IndexId);
				if (indexInstance != null)
					taskMetadata.IndexName = indexInstance.PublicName;
			}

			context.WriteJson(tasks);
		}
	}
}