using System;
using System.Linq;
using System.Reflection;
#if NETFX_CORE
using Raven.Client.WinRT.MissingFromWinRT;
#endif

namespace Raven.Abstractions.Logging.LogProviders
{
	public class NLogLogManager : LogManagerBase
	{
		private static bool providerIsAvailableOverride = true;
		private static readonly Lazy<Type> LazyGetLogManagerType = new Lazy<Type>(GetLogManagerTypeStatic, true);

		public NLogLogManager()
			: base(logger => new NLogLogger(logger))
		{
			if (!IsLoggerAvailable())
			{
				throw new InvalidOperationException("NLog.LogManager not found");
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

		private static Type GetLogManagerTypeStatic()
		{
#if !SL_4 && !NETFX_CORE
			Assembly nlogAssembly = GetNLogAssembly();
			return nlogAssembly != null ? nlogAssembly.GetType("NLog.LogManager") : Type.GetType("NLog.LogManager, nlog");
#else
			return Type.GetType("NLog.LogManager, nlog");
#endif
		}

		protected override Type GetNdcType()
		{
#if !SL_4 && !NETFX_CORE
			Assembly nlogAssembly = GetNLogAssembly();
			return nlogAssembly != null
				       ? nlogAssembly.GetType("NLog.NestedDiagnosticsContext")
					   : Type.GetType("NLog.NestedDiagnosticsContext, nlog");
#else
			return Type.GetType("NLog.NestedDiagnosticsContext, nlog");
#endif
		}

#if !SL_4 && !NETFX_CORE
		private static Assembly GetNLogAssembly()
		{
		    try
		    {
                return Assembly.Load("NLog");
		    }
		    catch (Exception)
		    {
		        return null;
		    }
		}
#endif

		protected override Type GetMdcType()
		{
#if !SL_4 && !NETFX_CORE
			Assembly log4NetAssembly = GetNLogAssembly();
			return log4NetAssembly != null ? log4NetAssembly.GetType("NLog.MappedDiagnosticsContext") : Type.GetType("NLog.MappedDiagnosticsContext, nlog");
#else
			return Type.GetType("NLog.MappedDiagnosticsContext, nlog");
#endif
		}

		public class NLogLogger : ILog
		{
			private readonly dynamic logger;

			internal NLogLogger(object logger)
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
					case LogLevel.Debug:
						if (logger.IsDebugEnabled)
						{
							logger.Debug(messageFunc());
						}
						break;
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
						if (logger.IsTraceEnabled)
						{
							logger.Trace(messageFunc());
						}
						break;
				}
			}

			public void Log<TException>(LogLevel logLevel, Func<string> messageFunc, TException exception)
				where TException : Exception
			{
				switch (logLevel)
				{
					case LogLevel.Debug:
						if (logger.IsDebugEnabled)
						{
							logger.DebugException(messageFunc(), exception);
						}
						break;
					case LogLevel.Info:
						if (logger.IsInfoEnabled)
						{
							logger.InfoException(messageFunc(), exception);
						}
						break;
					case LogLevel.Warn:
						if (logger.IsWarnEnabled)
						{
							logger.WarnException(messageFunc(), exception);
						}
						break;
					case LogLevel.Error:
						if (logger.IsErrorEnabled)
						{
							logger.ErrorException(messageFunc(), exception);
						}
						break;
					case LogLevel.Fatal:
						if (logger.IsFatalEnabled)
						{
							logger.FatalException(messageFunc(), exception);
						}
						break;
					default:
						if (logger.IsTraceEnabled)
						{
							logger.TraceException(messageFunc(), exception);
						}
						break;
				}
			}
		}
	}
}