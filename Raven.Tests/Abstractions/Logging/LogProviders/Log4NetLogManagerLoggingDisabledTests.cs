using System;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Logging.LogProviders;
using Xunit;
using log4net.Appender;
using log4net.Config;
using log4net.Filter;
using ILog = Raven.Abstractions.Logging.ILog;

namespace Raven.Tests.Abstractions.Logging.LogProviders
{
	public class Log4NetLogManagerLoggingDisabledTests : IDisposable
	{
		private readonly MemoryAppender memoryAppender;
		private readonly ILog sut;

		public Log4NetLogManagerLoggingDisabledTests()
		{
			Log4NetLogManager.ProviderIsAvailabileOverride = true;
			memoryAppender = new MemoryAppender();
			memoryAppender.AddFilter(new DenyAllFilter());
			BasicConfigurator.Configure(memoryAppender);
			sut = new Log4NetLogManager().GetLogger("Test");
		}

		public void Dispose()
		{
			log4net.LogManager.Shutdown();
		}

		[Fact]
		public void For_Trace_Then_should_not_log()
		{
			AssertShouldNotLog(LogLevel.Trace);
		}

		[Fact]
		public void For_Debug_Then_should_not_log()
		{
			AssertShouldNotLog(LogLevel.Debug);
		}

		[Fact]
		public void For_Info_Then_should_not_log()
		{
			AssertShouldNotLog(LogLevel.Info);
		}

		[Fact]
		public void For_Warn_Then_should_not_log()
		{
			AssertShouldNotLog(LogLevel.Warn);
		}

		[Fact]
		public void For_Error_Then_should_not_log()
		{
			AssertShouldNotLog(LogLevel.Error);
		}

		[Fact]
		public void For_Fatal_Then_should_not_log()
		{
			AssertShouldNotLog(LogLevel.Fatal);
		}

		private void AssertShouldNotLog(LogLevel logLevel)
		{
			sut.Log(logLevel, () => "m");
			sut.Log(logLevel, () => "m", new Exception("e"));
			Assert.Empty(memoryAppender.GetEvents());
		}
	}
}