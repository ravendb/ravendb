using Raven.Abstractions.Logging;
using Raven.Abstractions.Logging.LogProviders;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Abstractions.Logging
{
    public class LogManagerTests : NoDisposalNeeded
	{
		public LogManagerTests()
		{
			LogManager.CurrentLogManager = null;
		}
		
		[Fact]
		public void When_NLog_is_available_Then_should_get_NLogLogger()
		{
			NLogLogManager.ProviderIsAvailableOverride = true;
			Log4NetLogManager.ProviderIsAvailableOverride = true;
			ILog logger = LogManager.GetCurrentClassLogger();
			Assert.IsType<NLogLogManager.NLogLogger>(((LoggerExecutionWrapper) logger).WrappedLogger);
		}

		[Fact]
		public void When_Log4Net_is_available_Then_should_get_Log4NetLogger()
		{
			NLogLogManager.ProviderIsAvailableOverride = false;
			Log4NetLogManager.ProviderIsAvailableOverride = true;
			ILog logger = LogManager.GetLogger(GetType());
			Assert.IsType<Log4NetLogManager.Log4NetLogger>(((LoggerExecutionWrapper) logger).WrappedLogger);
		}

		[Fact]
		public void When_neither_NLog_or_Log4Net_is_available_Then_should_get_a_LoggerExecutionWrapper()
		{
			NLogLogManager.ProviderIsAvailableOverride = false;
			Log4NetLogManager.ProviderIsAvailableOverride = false;
			ILog logger = LogManager.GetLogger(GetType());
			Assert.IsType<LoggerExecutionWrapper>(logger);
		}

		[Fact]
		public void When_neither_NLog_or_Log4Net_is_available_Then_can_open_mapped_context()
		{
			NLogLogManager.ProviderIsAvailableOverride = false;
			Log4NetLogManager.ProviderIsAvailableOverride = false;
			Assert.NotNull(LogManager.OpenMappedContext("key", "value"));
		}

		[Fact]
		public void When_neither_NLog_or_Log4Net_is_available_Then_can_open_nested_context()
		{
			NLogLogManager.ProviderIsAvailableOverride = false;
			Log4NetLogManager.ProviderIsAvailableOverride = false;
			Assert.NotNull(LogManager.OpenNestedConext("context"));
		}

		[Fact]
		public void When_a_custom_target_is_registered_Then_should_log_to_target()
		{
			NLogLogManager.ProviderIsAvailableOverride = false;
			Log4NetLogManager.ProviderIsAvailableOverride = false;

			LogManager.RegisterTarget<TestTarget>();

			const string message = "message";
			ILog logger = LogManager.GetLogger(GetType());
			logger.Log(LogLevel.Error, () => message);

			Assert.Equal(message, LogManager.GetTarget<TestTarget>().LastMessage);
		}

		private class TestTarget : Target
		{
			internal string LastMessage { get; private set; }

			public override void Write(LogEventInfo logEvent)
			{
				LastMessage = logEvent.FormattedMessage;
			}
		}
	}
}