using System;
using System.Linq;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Logging.LogProviders;
using Xunit;
using log4net.Appender;
using log4net.Config;
using log4net.Core;
using LogManager = log4net.LogManager;

namespace Raven.Tests.Abstractions.Logging.LogProviders
{
	public class Log4NetLogManagerLoggingEnabledTests : IDisposable
	{
		private readonly MemoryAppender memoryAppender;
		private readonly ILog sut;

		public Log4NetLogManagerLoggingEnabledTests()
		{
			Log4NetLogManager.ProviderIsAvailabileOverride = true;
			memoryAppender = new MemoryAppender();
			BasicConfigurator.Configure(memoryAppender);
			sut = new Log4NetLogManager().GetLogger("Test");
		}

		public void Dispose()
		{
			LogManager.Shutdown();
		}

		private string GetSingleMessage()
		{
			LoggingEvent loggingEvent = memoryAppender.GetEvents().Single();
			return string.Format("{0}|{1}|{2}",
			                     loggingEvent.Level,
			                     loggingEvent.MessageObject,
			                     loggingEvent.ExceptionObject != null ? loggingEvent.ExceptionObject.Message : string.Empty);
		}

		[Fact]
		public void Should_be_able_to_get_IsWarnEnabled()
		{
			Assert.True(sut.IsWarnEnabled);
		}

		[Fact]
		public void Should_be_able_to_get_IsDebugEnabled()
		{
			Assert.True(sut.IsDebugEnabled);
		}

		[Fact]
		public void Should_be_able_to_log_trace_message()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Trace, () => "m");
			Assert.NotEmpty(memoryAppender.GetEvents());
			Assert.Equal("DEBUG|m|", GetSingleMessage()); //Trace messages in log4net are rendered as Debug
		}

		[Fact]
		public void Should_be_able_to_log_trace_exception()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Trace, () => "m", new Exception("e"));
			Assert.NotEmpty(memoryAppender.GetEvents());
			Assert.Equal("DEBUG|m|e", GetSingleMessage()); //Trace messages in log4net are rendered as Debug
		}

		[Fact]
		public void Should_be_able_to_log_debug_message()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Debug, () => "m");
			Assert.NotEmpty(memoryAppender.GetEvents());
			Assert.Equal("DEBUG|m|", GetSingleMessage());
		}

		[Fact]
		public void Should_be_able_to_log_debug_exception()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Debug, () => "m", new Exception("e"));
			Assert.NotEmpty(memoryAppender.GetEvents());
			Assert.Equal("DEBUG|m|e", GetSingleMessage());
		}

		[Fact]
		public void Should_be_able_to_log_info_message()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Info, () => "m");
			Assert.NotEmpty(memoryAppender.GetEvents());
			Assert.Equal("INFO|m|", GetSingleMessage());
		}

		[Fact]
		public void Should_be_able_to_log_info_exception()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Info, () => "m", new Exception("e"));
			Assert.NotEmpty(memoryAppender.GetEvents());
			Assert.Equal("INFO|m|e", GetSingleMessage());
		}

		[Fact]
		public void Should_be_able_to_log_warn_message()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Warn, () => "m");
			Assert.NotEmpty(memoryAppender.GetEvents());
			Assert.Equal("WARN|m|", GetSingleMessage());
		}

		[Fact]
		public void Should_be_able_to_log_warn_exception()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Warn, () => "m", new Exception("e"));
			Assert.NotEmpty(memoryAppender.GetEvents());
			Assert.Equal("WARN|m|e", GetSingleMessage());
		}

		[Fact]
		public void Should_be_able_to_log_error_message()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Error, () => "m");
			Assert.NotEmpty(memoryAppender.GetEvents());
			Assert.Equal("ERROR|m|", GetSingleMessage());
		}

		[Fact]
		public void Should_be_able_to_log_error_exception()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Error, () => "m", new Exception("e"));
			Assert.NotEmpty(memoryAppender.GetEvents());
			Assert.Equal("ERROR|m|e", GetSingleMessage());
		}

		[Fact]
		public void Should_be_able_to_log_fatal_message()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Fatal, () => "m");
			Assert.NotEmpty(memoryAppender.GetEvents());
			Assert.Equal("FATAL|m|", GetSingleMessage());
		}

		[Fact]
		public void Should_be_able_to_log_fatal_exception()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Fatal, () => "m", new Exception("e"));
			Assert.NotEmpty(memoryAppender.GetEvents());
			Assert.Equal("FATAL|m|e", GetSingleMessage());
		}
	}
}