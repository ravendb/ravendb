using System;
using System.Globalization;

namespace Raven.Abstractions.Logging
{
	public static class ILogExtensions
	{
		public static void ErrorException(this ILog logger, string message, Exception exception)
		{
			if (logger == null) throw new ArgumentException("logger is null", "logger");
			logger.Log(LogLevel.Error, () => message, exception);
		}

		public static void Debug(this ILog logger, Func<string> messageFactory)
		{
			if (logger == null) throw new ArgumentException("logger is null", "logger");
			logger.Log(LogLevel.Debug, messageFactory);
		}

		public static void Debug(this ILog logger, string message)
		{
			if (logger == null) throw new ArgumentException("logger is null", "logger");
			logger.Log(LogLevel.Debug, () => message);
		}

		public static void Debug(this ILog logger, string message, params object[] args)
		{
			if (logger == null) throw new ArgumentException("logger is null", "logger");
			logger.Log(LogLevel.Debug, () => string.Format(CultureInfo.InvariantCulture, message, args));
		}
	}
}