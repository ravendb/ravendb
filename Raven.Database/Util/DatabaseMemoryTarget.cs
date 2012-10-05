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
	public class DatabaseMemoryTarget : Target
	{
		private readonly ConcurrentDictionary<string, BoundedMemoryTarget> databaseTargets =
			new ConcurrentDictionary<string, BoundedMemoryTarget>();

		public BoundedMemoryTarget this[string databaseName]
		{
			get { return databaseTargets.GetOrAdd(databaseName, _ => new BoundedMemoryTarget()); }
		}

		public int DatabaseTargetCount
		{
			get { return databaseTargets.Count; }
		}

		public override void Write(LogEventInfo logEvent)
		{
			if (!logEvent.LoggerName.StartsWith("Raven."))
				return;
			string databaseName = LogContext.DatabaseName.Value;
			if (string.IsNullOrWhiteSpace(databaseName))
				return;
			BoundedMemoryTarget boundedMemoryTarget = databaseTargets.GetOrAdd(databaseName, _ => new BoundedMemoryTarget());
			boundedMemoryTarget.Write(logEvent);
		}

		public void Clear(string databaseName)
		{
			BoundedMemoryTarget _;
			databaseTargets.TryRemove(databaseName, out _);
		}

		public void ClearAll()
		{
			databaseTargets.Clear();
		}

		public class BoundedMemoryTarget
		{
			public const int Limit = 500;
			private ConcurrentQueue<LogEventInfo> generalLog = new ConcurrentQueue<LogEventInfo>();
			private ConcurrentQueue<LogEventInfo> warnLog = new ConcurrentQueue<LogEventInfo>();

			public IEnumerable<LogEventInfo> GeneralLog
			{
				get { return generalLog; }
			}

			public IEnumerable<LogEventInfo> WarnLog
			{
				get { return warnLog; }
			}

			internal void Write(LogEventInfo logEvent)
			{
				AddToQueue(logEvent, generalLog);
				if (logEvent.Level >= LogLevel.Warn)
					AddToQueue(logEvent, warnLog);
			}

			private static void AddToQueue(LogEventInfo logEvent, ConcurrentQueue<LogEventInfo> logEventInfos)
			{
				logEventInfos.Enqueue(logEvent);
				if (logEventInfos.Count <= Limit)
					return;

				LogEventInfo _;
				logEventInfos.TryDequeue(out _);
			}

			public void Clear()
			{
				generalLog = new ConcurrentQueue<LogEventInfo>();
				warnLog = new ConcurrentQueue<LogEventInfo>();
			}
		}
	}
}