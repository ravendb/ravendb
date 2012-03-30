using System;
using System.Diagnostics;

namespace Raven.Abstractions.Logging
{
	public static class LogProvider
	{
		private static ILogProvider currentLogProvider;

		public static ILog GetCurrentClassLogger()
		{
			return GetLogger(new StackFrame(2, false).GetMethod().DeclaringType);
		}

		public static ILog GetLogger(Type type)
		{
			return GetLogger(type.Name);
		}

		public static ILog GetLogger(string name)
		{
			var temp = currentLogProvider;
			return temp == null ? new NoOpLogger() : temp.GetLogger(name);
		}

		public static void SetCurrentLogProvider(ILogProvider logProvider)
		{
			currentLogProvider = logProvider;
		}

		private class NoOpLogger : ILog
		{
			public void Log(LogLevel logLevel, Func<string> messageFunc)
			{ }

			public void Log<TException>(LogLevel logLevel, Func<string> messageFunc, TException exception) where TException : Exception
			{ }
		}
	}
}