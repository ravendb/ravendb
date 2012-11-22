using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Abstractions.Logging.LogProviders;

namespace Raven.Abstractions.Logging
{
	public static class LogManager
	{
		private static readonly HashSet<Target> targets = new HashSet<Target>();

		public static void EnsureValidLogger()
		{
			GetLogger(typeof (LogManager));
		}

		public static ILog GetCurrentClassLogger()
		{
#if SILVERLIGHT
			var stackFrame = new StackTrace().GetFrame(1);
#else
			var stackFrame = new StackFrame(1, false);
#endif
			return GetLogger(stackFrame.GetMethod().DeclaringType);
		}

		private static ILogManager currentLogManager;
		public static ILogManager CurrentLogManager
		{
			get { return currentLogManager ?? (currentLogManager = ResolveExtenalLogManager()); }
			set { currentLogManager = value; }
		}

		public static ILog GetLogger(Type type)
		{
			return GetLogger(type.FullName);
		}

		public static ILog GetLogger(string name)
		{
			ILogManager logManager = CurrentLogManager;
			if (logManager == null)
				return new LoggerExecutionWrapper(new NoOpLogger(), name, targets.ToArray());
			
			// This can throw in a case of invalid NLog.config file.
			var log = logManager.GetLogger(name);
			return new LoggerExecutionWrapper(log, name, targets.ToArray());
		}

		private static ILogManager ResolveExtenalLogManager()
		{
			if (NLogLogManager.IsLoggerAvailable())
			{
				return new NLogLogManager();
			}
			if (Log4NetLogManager.IsLoggerAvailable())
			{
				return new Log4NetLogManager();
			}
			return null;
		}

		public static void RegisterTarget<T>() where T: Target, new()
		{
			targets.Add(new T());
		}

		public static T GetTarget<T>() where T: Target
		{
			return targets.ToArray().OfType<T>().FirstOrDefault();
		}

		public class NoOpLogger : ILog
		{
			public bool IsDebugEnabled { get { return false; } }

			public bool IsWarnEnabled { get { return false; } }

			public void Log(LogLevel logLevel, Func<string> messageFunc)
			{}

			public void Log<TException>(LogLevel logLevel, Func<string> messageFunc, TException exception)
				where TException : Exception
			{}
		}
	}

	public abstract class Target
	{
		public abstract void Write(LogEventInfo logEvent);
	}

	public class LogEventInfo
	{
		public LogLevel Level { get; set; }
		public DateTime TimeStamp { get; set; }
		public string FormattedMessage { get; set; }
		public string LoggerName { get; set; }
		public Exception Exception { get; set; }
	}
}