namespace Raven.Tests.Abstractions.Logging
{
	using Raven.Abstractions.Logging;
	using Raven.Abstractions.Logging.LogProviders;
	using Xunit;

	public class LogManagerTests
	{
		[Fact]
		public void When_NLog_is_available_Then_should_get_NLogLogger()
		{
			LogManager.SetCurrentLogManager(null);
			NLogLogManager.ProviderIsAvailabileOverride = true;
			Log4NetLogManager.ProviderIsAvailabileOverride = true;
			ILog logger = LogManager.GetCurrentClassLogger();
			Assert.IsType<NLogLogManager.NLogLogger>(((LoggerExecutionWrapper)logger).WrappedLogger);

			NLogLogManager.ProviderIsAvailabileOverride = true;
			Log4NetLogManager.ProviderIsAvailabileOverride = true;
		}

		[Fact]
		public void When_Log4Net_is_available_Then_should_get_Log4NetLogger()
		{
			LogManager.SetCurrentLogManager(null);
			NLogLogManager.ProviderIsAvailabileOverride = false;
			Log4NetLogManager.ProviderIsAvailabileOverride = true;
			ILog logger = LogManager.GetLogger(GetType());
			Assert.IsType<Log4NetLogManager.Log4NetLogger>(((LoggerExecutionWrapper)logger).WrappedLogger);

			NLogLogManager.ProviderIsAvailabileOverride = true;
			Log4NetLogManager.ProviderIsAvailabileOverride = true;
		}

		[Fact]
		public void When_neither_NLog_or_Log4Net_is_available_Then_should_get_NoOpLogger()
		{
			LogManager.SetCurrentLogManager(null);
			NLogLogManager.ProviderIsAvailabileOverride = false;
			Log4NetLogManager.ProviderIsAvailabileOverride = false;
			ILog logger = LogManager.GetLogger(GetType());
			Assert.IsType<LogManager.NoOpLogger>(logger);

			NLogLogManager.ProviderIsAvailabileOverride = true;
			Log4NetLogManager.ProviderIsAvailabileOverride = true;
		}
	}
}