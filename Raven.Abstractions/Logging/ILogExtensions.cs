namespace Raven.Abstractions.Logging
{
	using System;
	using System.Globalization;

	public static class ILogExtensions
	{
		public static void Debug(this ILog logger, Func<string> messageFunc)
		{
			GuardAgainstNullLogger(logger);
			logger.Log(LogLevel.Debug, messageFunc);
		}

		public static void Debug(this ILog logger, string message, params object[] args)
		{
			GuardAgainstNullLogger(logger);
			logger.Log(LogLevel.Debug, () =>
			{
				if (args == null || args.Length == 0)
					return message;
				return string.Format(CultureInfo.InvariantCulture, message, args);
			});
		}

		public static void DebugException(this ILog logger, string message, Exception ex)
		{
			GuardAgainstNullLogger(logger);
			logger.Log(LogLevel.Debug, () => message, ex);
		}

		public static void Error(this ILog logger, string message, params object[] args)
		{
			GuardAgainstNullLogger(logger);
			logger.Log(LogLevel.Error, () =>
			{
				if (args == null || args.Length == 0)
					return message;
				return string.Format(CultureInfo.InvariantCulture, message, args);
			});
		}

		public static void ErrorException(this ILog logger, string message, Exception exception)
		{
			GuardAgainstNullLogger(logger);
			logger.Log(LogLevel.Error, () => message, exception);
		}

		public static void FatalException(this ILog logger, string message, Exception exception)
		{
			GuardAgainstNullLogger(logger);
			logger.Log(LogLevel.Fatal, () => message, exception);
		}

		public static void Info(this ILog logger, Func<string> messageFunc)
		{
			GuardAgainstNullLogger(logger);
			logger.Log(LogLevel.Info, messageFunc);
		}

		public static void Info(this ILog logger, string message, params object[] args)
		{
			GuardAgainstNullLogger(logger);
			logger.Log(LogLevel.Info, () =>
			{
				if (args == null || args.Length == 0)
					return message;
				return string.Format(CultureInfo.InvariantCulture, message, args);
			});
		}

		public static void InfoException(this ILog logger, string message, Exception exception)
		{
			GuardAgainstNullLogger(logger);
			logger.Log(LogLevel.Info, () => message, exception);
		}

		public static void Warn(this ILog logger, Func<string> messageFunc)
		{
			GuardAgainstNullLogger(logger);
			logger.Log(LogLevel.Warn, messageFunc);
		}

		public static void Warn(this ILog logger, string message, params object[] args)
		{
			GuardAgainstNullLogger(logger);
			logger.Log(LogLevel.Warn, () =>
			{
				if (args == null || args.Length == 0)
					return message;
				return string.Format(CultureInfo.InvariantCulture, message, args);
			});
		}

		public static void WarnException(this ILog logger, string message, Exception ex)
		{
			GuardAgainstNullLogger(logger);
			logger.Log(LogLevel.Warn, () => message, ex);
		}

		private static void GuardAgainstNullLogger(ILog logger)
		{
			if(logger == null)
			{
				throw new ArgumentException("logger is null", "logger");
			}
		}
	}
}