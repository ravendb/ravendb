using System;
using System.Xml;
using NLog;
using NLog.Config;
using Raven.Abstractions.Logging;
using Raven.Database.Server;
using LogLevel = Raven.Abstractions.Logging.LogLevel;

namespace Raven.Tests
{
	public class WithNLog
	{
		static WithNLog()
		{
			if (NLog.LogManager.Configuration != null)
				return;

			HttpEndpointRegistration.RegisterHttpEndpointTarget();

			using (var stream = typeof (RemoteClientTest).Assembly.GetManifestResourceStream("Raven.Tests.DefaultLogging.config")
				)
			using (var reader = XmlReader.Create(stream))
			{
				NLog.LogManager.Configuration = new XmlLoggingConfiguration(reader, "default-config");
			}

			LogProvider.SetCurrentLogProvider(new NLogProvider());
		}

		private class NLogProvider : ILogProvider
		{
			public ILog GetLogger(string name)
			{
				return new NLogLogger(NLog.LogManager.GetLogger(name));
			}

			private class NLogLogger : ILog
			{
				private readonly Logger _logger;

				public NLogLogger(Logger logger)
				{
					_logger = logger;
				}

				public void Log(LogLevel logLevel, Func<string> messageFunc)
				{
					switch (logLevel)
					{
						case LogLevel.Trace:
							if (_logger.IsDebugEnabled)
								_logger.Debug(messageFunc());
							break;
						case LogLevel.Debug:
							if (_logger.IsDebugEnabled)
								_logger.Debug(messageFunc());
							break;
						case LogLevel.Info:
							if (_logger.IsInfoEnabled)
								_logger.Info(messageFunc());
							break;
						case LogLevel.Warn:
							if (_logger.IsWarnEnabled)
								_logger.Warn(messageFunc());
							break;
						case LogLevel.Error:
							if (_logger.IsErrorEnabled)
								_logger.Error(messageFunc());
							break;
						case LogLevel.Fatal:
							if (_logger.IsFatalEnabled)
								_logger.Fatal(messageFunc());
							break;
						default:
							throw new ArgumentOutOfRangeException("logLevel");
					}
				}

				public void Log<TException>(LogLevel logLevel, Func<string> messageFunc, TException exception)
					where TException : Exception
				{
					switch (logLevel)
					{
						case LogLevel.Trace:
							if (_logger.IsTraceEnabled)
								_logger.TraceException(messageFunc(), exception);
							break;
						case LogLevel.Debug:
							if (_logger.IsDebugEnabled)
								_logger.DebugException(messageFunc(), exception);
							break;
						case LogLevel.Info:
							if (_logger.IsInfoEnabled)
								_logger.InfoException(messageFunc(), exception);
							break;
						case LogLevel.Warn:
							if (_logger.IsWarnEnabled)
								_logger.WarnException(messageFunc(), exception);
							break;
						case LogLevel.Error:
							if (_logger.IsErrorEnabled)
								_logger.ErrorException(messageFunc(), exception);
							break;
						case LogLevel.Fatal:
							if (_logger.IsFatalEnabled)
								_logger.FatalException(messageFunc(), exception);
							break;
						default:
							throw new ArgumentOutOfRangeException("logLevel");
					}
				}
			}
		}
	}
}