namespace Raven.Abstractions.Logging.LogProviders
{
	using System;
	using System.Linq;
	using System.Linq.Expressions;
	using System.Reflection;

	public class Log4NetLogManager : ILogManager
	{
		private readonly Func<string, object> getLoggerByNameDelegate;
		private static bool providerIsAvailabileOverride = true;
		private static readonly Lazy<Type> LazyGetLogManagerType = new Lazy<Type>(GetLogManagerType, true); 

		public Log4NetLogManager()
		{
			if (!IsLoggerAvailable())
			{
				throw new InvalidOperationException("log4net.LogManager not found");
			}
			getLoggerByNameDelegate = GetGetLoggerMethodCall();
		}

		public static bool ProviderIsAvailabileOverride
		{
			get { return providerIsAvailabileOverride; }
			set { providerIsAvailabileOverride = value; }
		}

		public ILog GetLogger(string name)
		{
			return new Log4NetLogger(getLoggerByNameDelegate(name));
		}

		public static bool IsLoggerAvailable()
		{
			return ProviderIsAvailabileOverride && LazyGetLogManagerType.Value != null;
		}

		private static Type GetLogManagerType()
		{
#if !SL_4
			Assembly[] assemblies = AppDomain.CurrentDomain.GetAssemblies();
			Assembly nlogAssembly = assemblies.FirstOrDefault(assembly => assembly.FullName.StartsWith("log4net,"));
			return nlogAssembly != null ? nlogAssembly.GetType("log4net.LogManager") : Type.GetType("log4net.LogManager, log4net");
#else
			return Type.GetType("NLog.LogManager, log4net");
#endif
		}

		private static Func<string, object> GetGetLoggerMethodCall()
		{
			Type logManagerType = GetLogManagerType();
			MethodInfo method = logManagerType.GetMethod("GetLogger", new[] {typeof(string)});
			ParameterExpression resultValue;
			ParameterExpression keyParam = Expression.Parameter(typeof(string), "key");
			MethodCallExpression methodCall = Expression.Call(null, method, new Expression[] {resultValue = keyParam});
			return Expression.Lambda<Func<string, object>>(methodCall, new[] {resultValue}).Compile();
		}

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
							logger.Debug(messageFunc()); // Log4Net doesn't have a 'Trace' level, so all Trace messages are written as 'Debug'
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