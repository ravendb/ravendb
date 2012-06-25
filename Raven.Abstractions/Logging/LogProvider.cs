namespace Raven.Abstractions.Logging
{
	using System;
	using System.Diagnostics;
	using Raven.Abstractions.Logging.LogProviders;

	public static class LogProvider
	{
		private static ILogProvider currentLogProvider;

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
			ILogProvider temp = currentLogProvider ?? ResolveLogProvider();
			return temp == null ? new NoOpLogger() : (ILog)new LoggerExecutionWrapper(temp.GetLogger(name));
		}

		public static void SetCurrentLogProvider(ILogProvider logProvider)
		{
			currentLogProvider = logProvider;
		}

		private static ILogProvider ResolveLogProvider()
		{
			if (NLogLogProvider.IsLoggerAvailable())
			{
				return new NLogLogProvider();
			}
			if (Log4NetLogProvider.IsLoggerAvailable())
			{
				return new Log4NetLogProvider();
			}
			return null;
		}

		public class NoOpLogger : ILog
		{
			public void Log(LogLevel logLevel, Func<string> messageFunc)
			{}

			public void Log<TException>(LogLevel logLevel, Func<string> messageFunc, TException exception)
				where TException : Exception
			{}
		}
	}
}