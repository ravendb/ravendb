namespace Raven.Abstractions.Logging.LogProviders
{
	using System;
	using System.Linq;
	using System.Linq.Expressions;
	using System.Reflection;

	public class NLogLogManager : ILogManager
	{
		private readonly Func<string, object> getLoggerByNameDelegate;
		private static bool providerIsAvailabileOverride = true;
		private static readonly Lazy<Type> LazyGetLogManagerType = new Lazy<Type>(GetLogManagerType, true); 
		
		public NLogLogManager()
		{
			if(!IsLoggerAvailable())
			{
				throw new InvalidOperationException("NLog.LogManager not found");
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
			return new NLogLogger(getLoggerByNameDelegate(name));
		}

		public static bool IsLoggerAvailable()
		{
			return ProviderIsAvailabileOverride && LazyGetLogManagerType.Value != null;
		}

		private static Type GetLogManagerType()
		{
#if !SL_4
			Assembly[] assemblies = AppDomain.CurrentDomain.GetAssemblies();
			Assembly nlogAssembly = assemblies.FirstOrDefault(assembly => assembly.FullName.StartsWith("NLog,"));
			return nlogAssembly != null ? nlogAssembly.GetType("NLog.LogManager") : Type.GetType("NLog.LogManager, nlog");
#else
			return Type.GetType("NLog.LogManager, nlog");
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
				switch(logLevel)
				{
					case LogLevel.Debug:
						if(logger.IsDebugEnabled)
						{
							logger.Debug(messageFunc());
						}
						break;
					case LogLevel.Info:
						if(logger.IsInfoEnabled)
						{
							logger.Info(messageFunc());
						}
						break;
					case LogLevel.Warn:
						if(logger.IsWarnEnabled)
						{
							logger.Warn(messageFunc());
						}
						break;
					case LogLevel.Error:
						if(logger.IsErrorEnabled)
						{
							logger.Error(messageFunc());
						}
						break;
					case LogLevel.Fatal:
						if(logger.IsFatalEnabled)
						{
							logger.Fatal(messageFunc());
						}
						break;
					default:
						if(logger.IsTraceEnabled)
						{
							logger.Trace(messageFunc());
						}
						break;
				}
			}

			public void Log<TException>(LogLevel logLevel, Func<string> messageFunc, TException exception)
				where TException : Exception
			{
				switch(logLevel)
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