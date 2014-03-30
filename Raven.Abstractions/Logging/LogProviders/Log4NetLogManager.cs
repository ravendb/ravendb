using System;
using System.Linq;
using System.Reflection;
#if NETFX_CORE
using Raven.Client.WinRT.MissingFromWinRT;
#endif

namespace Raven.Abstractions.Logging.LogProviders
{
	public class Log4NetLogManager : LogManagerBase
	{
		private static bool providerIsAvailableOverride = true;
		private static readonly Lazy<Type> LazyGetLogManagerType = new Lazy<Type>(GetLogManagerTypeStatic, true);

		public Log4NetLogManager()
			: base(logger => new Log4NetLogger(logger))
		{
			if (!IsLoggerAvailable())
			{
				throw new InvalidOperationException("log4net.LogManager not found");
			}
		}

		public static bool ProviderIsAvailableOverride
		{
			get { return providerIsAvailableOverride; }
			set { providerIsAvailableOverride = value; }
		}

		public static bool IsLoggerAvailable()
		{
			return ProviderIsAvailableOverride && LazyGetLogManagerType.Value != null;
		}

		protected override Type GetLogManagerType()
		{
			return GetLogManagerTypeStatic();
		}

		protected static Type GetLogManagerTypeStatic()
		{
#if !SL_4 && !NETFX_CORE
			Assembly log4NetAssembly = GetLog4NetAssembly();
			return log4NetAssembly != null
				       ? log4NetAssembly.GetType("log4net.LogManager")
				       : Type.GetType("log4net.LogManager, log4net");
#else
			return Type.GetType("log4net.LogManager, log4net");
#endif
		}

		protected override Type GetNdcType()
		{
#if !SL_4 && !NETFX_CORE
			Assembly log4NetAssembly = GetLog4NetAssembly();
			return log4NetAssembly != null ? log4NetAssembly.GetType("log4net.NDC") : Type.GetType("log4net.NDC, log4net");
#else
			return Type.GetType("log4net.NDC, log4net");
#endif
		}

		protected override Type GetMdcType()
		{
#if !SL_4 && !NETFX_CORE
			Assembly log4NetAssembly = GetLog4NetAssembly();
			return log4NetAssembly != null ? log4NetAssembly.GetType("log4net.MDC") : Type.GetType("log4net.MDC, log4net");
#else
			return Type.GetType("log4net.MDC, log4net");
#endif
		}

#if !SL_4 && !NETFX_CORE
		private static Assembly GetLog4NetAssembly()
		{
            try
            {
                return Assembly.Load("log4net");
            }
            catch (Exception)
            {
                return null;
            }
		}
#endif

		public class Log4NetLogger : ILog
		{
			private readonly dynamic logger;

			internal Log4NetLogger(object logger)
			{
				this.logger = logger;
			}

			public bool IsDebugEnabled
			{
				get { return logger.IsDebugEnabled; }
			}

			public bool IsWarnEnabled
			{
				get { return logger.IsWarnEnabled; }
			}

			public void Log(LogLevel logLevel, Func<string> messageFunc)
			{
				switch (logLevel)
				{
					case LogLevel.Info:
						if (logger.IsInfoEnabled)
						{
							logger.Info(messageFunc());
						}
						break;
					case LogLevel.Warn:
						if (logger.IsWarnEnabled)
						{
							logger.Warn(messageFunc());
						}
						break;
					case LogLevel.Error:
						if (logger.IsErrorEnabled)
						{
							logger.Error(messageFunc());
						}
						break;
					case LogLevel.Fatal:
						if (logger.IsFatalEnabled)
						{
							logger.Fatal(messageFunc());
						}
						break;
					default:
						if (logger.IsDebugEnabled)
						{
							logger.Debug(messageFunc());
								// Log4Net doesn't have a 'Trace' level, so all Trace messages are written as 'Debug'
						}
						break;
				}
			}

			public void Log<TException>(LogLevel logLevel, Func<string> messageFunc, TException exception)
				where TException : Exception
			{
				switch (logLevel)
				{
					case LogLevel.Info:
						if (logger.IsDebugEnabled)
						{
							logger.Info(messageFunc(), exception);
						}
						break;
					case LogLevel.Warn:
						if (logger.IsWarnEnabled)
						{
							logger.Warn(messageFunc(), exception);
						}
						break;
					case LogLevel.Error:
						if (logger.IsErrorEnabled)
						{
							logger.Error(messageFunc(), exception);
						}
						break;
					case LogLevel.Fatal:
						if (logger.IsFatalEnabled)
						{
							logger.Fatal(messageFunc(), exception);
						}
						break;
					default:
						if (logger.IsDebugEnabled)
						{
							logger.Debug(messageFunc(), exception);
						}
						break;
				}
			}
		}
	}
}