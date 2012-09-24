using Raven.Abstractions.Logging;
using Raven.Abstractions.Logging.LogProviders;
using Xunit;

namespace Raven.Tests.Abstractions.Logging
{
	public class LogManagerTests
	{
		[Fact]
		public void When_NLog_is_available_Then_should_get_NLogLogger()
		{
			LogManager.SetCurrentLogManager(null);
			NLogLogManager.ProviderIsAvailabileOverride = true;
			Log4NetLogManager.ProviderIsAvailabileOverride = true;
			ILog logger = LogManager.GetCurrentClassLogger();
			Assert.IsType<NLogLogManager.NLogLogger>(((LoggerExecutionWrapper) logger).WrappedLogger);
		}

		[Fact]
		public void When_Log4Net_is_available_Then_should_get_Log4NetLogger()
		{
			LogManager.SetCurrentLogManager(null);
			NLogLogManager.ProviderIsAvailabileOverride = false;
			Log4NetLogManager.ProviderIsAvailabileOverride = true;
			ILog logger = LogManager.GetLogger(GetType());
			Assert.IsType<Log4NetLogManager.Log4NetLogger>(((LoggerExecutionWrapper) logger).WrappedLogger);
		}

		[Fact]
		public void When_neither_NLog_or_Log4Net_is_available_Then_should_get_a_LoggerExecutionWrapper()
		{
			LogManager.SetCurrentLogManager(null);
			NLogLogManager.ProviderIsAvailabileOverride = false;
			Log4NetLogManager.ProviderIsAvailabileOverride = false;
			ILog logger = LogManager.GetLogger(GetType());
			Assert.IsType<LoggerExecutionWrapper>(logger);
		}

		[Fact]
		public void When_a_custom_target_is_registered_Then_should_log_to_target()
		{
			LogManager.SetCurrentLogManager(null);
			NLogLogManager.ProviderIsAvailabileOverride = false;
			Log4NetLogManager.ProviderIsAvailabileOverride = false;

			LogManager.RegisterTarget<TestTarget>();

			const string message = "message";
			ILog logger = LogManager.GetLogger(GetType());
			logger.Log(LogLevel.Debug, () => message);

			Assert.Equal(message, LogManager.GetTarget<TestTarget>().LastMessage);
		}

		#region Nested type: TestTarget

		private class TestTarget : Target
		{
			internal string LastMessage { get; private set; }

			public override void Write(LogEventInfo logEvent)
			{
				LastMessage = logEvent.FormattedMessage;
			}
		}

		#endregion
	}
}