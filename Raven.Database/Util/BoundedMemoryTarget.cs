//-----------------------------------------------------------------------
// <copyright company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Concurrent;
using System.Collections.Generic;
using Raven.Abstractions.Logging;

namespace Raven.Database.Util
{
	public class BoundedMemoryTarget : Target
	{
		private ConcurrentQueue<LogEventInfo> generalLog = new ConcurrentQueue<LogEventInfo>();
		private ConcurrentQueue<LogEventInfo> warnLog = new ConcurrentQueue<LogEventInfo>();

		public override void Write(LogEventInfo logEvent)
		{
			if (!logEvent.LoggerName.StartsWith("Raven."))
				return;
			AddToQueue(logEvent, generalLog);
			if(logEvent.Level>=LogLevel.Warn)
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

		public IEnumerable<LogEventInfo> GeneralLog
		{
			get { return generalLog; }
		}

		public IEnumerable<LogEventInfo> WarnLog
		{
			get { return warnLog; }
		}

		public void Clear()
		{
			generalLog = new ConcurrentQueue<LogEventInfo>();
			warnLog = new ConcurrentQueue<LogEventInfo>();
		}
	}
}