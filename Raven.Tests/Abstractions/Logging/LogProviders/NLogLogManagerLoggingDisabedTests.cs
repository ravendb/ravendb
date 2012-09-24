using System;
using Raven.Abstractions.Logging.LogProviders;
using NLog.Config;
using NLog.Targets;
using Xunit;
using LogLevel = NLog.LogLevel;

namespace Raven.Tests.Abstractions.Logging.LogProviders
{
	public class NLogLogManagerLoggingDisabedTests : IDisposable
	{
		private Raven.Abstractions.Logging.ILog sut;
		private MemoryTarget target;

		private void ConfigureLogger(NLog.LogLevel nlogLogLevel)
		{
			NLogLogManager.ProviderIsAvailabileOverride = true;
			var config = new LoggingConfiguration();
			target = new MemoryTarget();
			target.Layout = "${level:uppercase=true}|${message}|${exception}";
			config.AddTarget("memory", target);
			var loggingRule = new LoggingRule("*", LogLevel.Trace, target);
			loggingRule.DisableLoggingForLevel(nlogLogLevel);
			config.LoggingRules.Add(loggingRule);
			NLog.LogManager.Configuration = config;
			sut = new NLogLogManager().GetLogger("Test");
		}

		[Fact]
		public void For_Trace_Then_should_not_log()
		{
			ConfigureLogger(LogLevel.Trace);
			AssertShouldNotLog(Raven.Abstractions.Logging.LogLevel.Trace);
		}

		[Fact]
		public void For_Debug_Then_should_not_log()
		{
			ConfigureLogger(LogLevel.Debug);
			AssertShouldNotLog(Raven.Abstractions.Logging.LogLevel.Debug);
		}

		[Fact]
		public void For_Info_Then_should_not_log()
		{
			ConfigureLogger(LogLevel.Info);
			AssertShouldNotLog(Raven.Abstractions.Logging.LogLevel.Info);
		}

		[Fact]
		public void For_Warn_Then_should_not_log()
		{
			ConfigureLogger(LogLevel.Warn);
			AssertShouldNotLog(Raven.Abstractions.Logging.LogLevel.Warn);
		}

		[Fact]
		public void For_Error_Then_should_not_log()
		{
			ConfigureLogger(LogLevel.Error);
			AssertShouldNotLog(Raven.Abstractions.Logging.LogLevel.Error);
		}

		[Fact]
		public void For_Fatal_Then_should_not_log()
		{
			ConfigureLogger(LogLevel.Fatal);
			AssertShouldNotLog(Raven.Abstractions.Logging.LogLevel.Fatal);
		}

		private void AssertShouldNotLog(Raven.Abstractions.Logging.LogLevel logLevel)
		{
			sut.Log(logLevel, () => "m");
			sut.Log(logLevel, () => "m", new Exception("e"));
			Assert.Empty(target.Logs);
		}

		public void Dispose()
		{
			NLog.LogManager.Configuration = null;
		}
	}
}