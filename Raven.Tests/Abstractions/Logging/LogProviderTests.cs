namespace Raven.Tests.Abstractions.Logging
{
	using Raven.Abstractions.Logging;
	using Raven.Abstractions.Logging.LogProviders;
	using Xunit;

	public class LogProviderTests
	{
		[Fact]
		public void When_NLog_is_available_Then_should_get_NLogLogger()
		{
			LogProvider.SetCurrentLogProvider(null);
			NLogLogProvider.ProviderIsAvailabileOverride = true;
			Log4NetLogProvider.ProviderIsAvailabileOverride = true;
			ILog logger = LogProvider.GetCurrentClassLogger();
			Assert.IsType<NLogLogProvider.NLogLogger>(((LoggerExecutionWrapper)logger).WrappedLogger);

			NLogLogProvider.ProviderIsAvailabileOverride = true;
			Log4NetLogProvider.ProviderIsAvailabileOverride = true;
		}

		[Fact]
		public void When_Log4Net_is_available_Then_should_get_Log4NetLogger()
		{
			LogProvider.SetCurrentLogProvider(null);
			NLogLogProvider.ProviderIsAvailabileOverride = false;
			Log4NetLogProvider.ProviderIsAvailabileOverride = true;
			ILog logger = LogProvider.GetLogger(GetType());
			Assert.IsType<Log4NetLogProvider.Log4NetLogger>(((LoggerExecutionWrapper)logger).WrappedLogger);

			NLogLogProvider.ProviderIsAvailabileOverride = true;
			Log4NetLogProvider.ProviderIsAvailabileOverride = true;
		}

		[Fact]
		public void When_neither_NLog_or_Log4Net_is_available_Then_should_get_NoOpLogger()
		{
			LogProvider.SetCurrentLogProvider(null);
			NLogLogProvider.ProviderIsAvailabileOverride = false;
			Log4NetLogProvider.ProviderIsAvailabileOverride = false;
			ILog logger = LogProvider.GetLogger(GetType());
			Assert.IsType<LogProvider.NoOpLogger>(logger);

			NLogLogProvider.ProviderIsAvailabileOverride = true;
			Log4NetLogProvider.ProviderIsAvailabileOverride = true;
		}
	}
}