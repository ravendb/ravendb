// //-----------------------------------------------------------------------
// // <copyright company="Hibernating Rhinos LTD">
// //     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// // </copyright>
// //-----------------------------------------------------------------------
using System.Collections.Concurrent;
using System.Collections.Generic;
using NLog;
using NLog.Targets;

namespace Raven.Database.Util
{
	public class BoundedMemoryTarget : Target
	{
		private readonly ConcurrentQueue<LogEventInfo> items = new ConcurrentQueue<LogEventInfo>();

		protected override void Write(LogEventInfo logEvent)
		{
			items.Enqueue(logEvent);
			if (items.Count <= 500)
				return;

			LogEventInfo _;
			items.TryDequeue(out _);
		}

		public IEnumerable<LogEventInfo> GetSnapshot()
		{
			return items;
		}
	}
}