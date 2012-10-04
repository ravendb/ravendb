//-----------------------------------------------------------------------
// <copyright company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Concurrent;
using System.Collections.Generic;
using Raven.Abstractions.Logging;
using Raven.Database.Server;

namespace Raven.Database.Util
{
	public class DatabaseTarget : Target
	{
		private readonly ConcurrentDictionary<string, BoundedMemoryTarget> databaseTargets = new ConcurrentDictionary<string,BoundedMemoryTarget>(); 

		public override void Write(LogEventInfo logEvent)
		{
			if (!logEvent.LoggerName.StartsWith("Raven."))
				return;
			string databaseName = CurrentOperationContext.DatabaseName.Value;
			if (databaseName == null)
				return;
			BoundedMemoryTarget boundedMemoryTarget = databaseTargets.GetOrAdd(databaseName, _ => new BoundedMemoryTarget());
			boundedMemoryTarget.Write(logEvent);
		}

		internal BoundedMemoryTarget this[string databaseName]
		{
			get
			{
				BoundedMemoryTarget boundedMemoryTarget;
				databaseTargets.TryGetValue(databaseName, out boundedMemoryTarget);
				return boundedMemoryTarget;
			}
		}

		internal class BoundedMemoryTarget
		{
			private ConcurrentQueue<LogEventInfo> generalLog = new ConcurrentQueue<LogEventInfo>();
			private ConcurrentQueue<LogEventInfo> warnLog = new ConcurrentQueue<LogEventInfo>();

			internal void Write(LogEventInfo logEvent)
			{
				AddToQueue(logEvent, generalLog);
				if (logEvent.Level >= LogLevel.Warn)
					AddToQueue(logEvent, warnLog);
			}

			private static void AddToQueue(LogEventInfo logEvent, ConcurrentQueue<LogEventInfo> logEventInfos)
			{
				logEventInfos.Enqueue(logEvent);
				if (logEventInfos.Count <= 500)
					return;

				LogEventInfo _;
				logEventInfos.TryDequeue(out _);
			}

			internal IEnumerable<LogEventInfo> GeneralLog
			{
				get { return generalLog; }
			}

			internal IEnumerable<LogEventInfo> WarnLog
			{
				get { return warnLog; }
			}

			public void Clear()
			{
				generalLog = new ConcurrentQueue<LogEventInfo>();
				warnLog = new ConcurrentQueue<LogEventInfo>();
			}
		}

		public void Clear()
		{
			foreach (var boundedMemoryTarget in databaseTargets.Values)
			{
				boundedMemoryTarget.Clear();
			}
		}
	}
}