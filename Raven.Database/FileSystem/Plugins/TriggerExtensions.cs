// -----------------------------------------------------------------------
//  <copyright file="TriggerExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Plugins
{
	public static class TriggerExtensions
	{
		private static readonly ILog Log = LogManager.GetCurrentClassLogger();

		public static bool CanReadFile(this OrderedPartCollection<AbstractFileReadTrigger> triggers, string name, RavenJObject metadata, ReadOperation operation)
		{
			foreach (var trigger in triggers)
			{
				var result = trigger.Value.AllowRead(name, metadata, operation);
				if (result.Veto == ReadVetoResult.ReadAllow.Allow)
					continue;

				Log.Debug("Trigger {0} asked us to ignore {1}", trigger.Value, name);
				return false;
			}

			return true;
		}
	}
}