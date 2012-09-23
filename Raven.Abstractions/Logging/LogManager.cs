namespace Raven.Abstractions.Logging
{
	using System;
	using System.Diagnostics;
	using LogProviders;

	public static class LogManager
	{
		private static ILogManager currentLogManager;

		public static ILog GetCurrentClassLogger()
		{
#if SILVERLIGHT
			var stackFrame = new StackTrace().GetFrame(1);
#else
			var stackFrame = new StackFrame(1, false);
#endif
			return GetLogger(stackFrame.GetMethod().DeclaringType);
		}

		public static ILog GetLogger(Type type)
		{
			return GetLogger(type.FullName);
		}

		public static ILog GetLogger(string name)
		{
			ILogManager temp = currentLogManager ?? ResolveLogProvider();
			return temp == null ? new NoOpLogger() : (ILog)new LoggerExecutionWrapper(temp.GetLogger(name));
		}

		public static void SetCurrentLogManager(ILogManager logManager)
		{
			currentLogManager = logManager;
		}

		private static ILogManager ResolveLogProvider()
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
}