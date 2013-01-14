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
		private readonly Log4NetLogManager log4NetLogManager;
		private readonly MemoryAppender memoryAppender;
		private readonly ILog sut;

		public Log4NetLogManagerLoggingEnabledTests()
		{
			Log4NetLogManager.ProviderIsAvailableOverride = true;
			memoryAppender = new MemoryAppender();
			BasicConfigurator.Configure(memoryAppender);
			log4NetLogManager = new Log4NetLogManager();
			sut = log4NetLogManager.GetLogger("Test");
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

		private string GetSingleMessageWithNestedContext()
		{
			LoggingEvent loggingEvent = memoryAppender.GetEvents().Single();
			return string.Format("{0}|{1}|{2}|{3}",
								 loggingEvent.Level,
								 loggingEvent.Properties.Contains("NDC") ?  loggingEvent.Properties["NDC"] : string.Empty,
								 loggingEvent.MessageObject,
								 loggingEvent.ExceptionObject != null ? loggingEvent.ExceptionObject.Message : string.Empty);
		}

		private string GetSingleMessageWithMappedContext(string key)
		{
			LoggingEvent loggingEvent = memoryAppender.GetEvents().Single();
			return string.Format("{0}|{1}|{2}|{3}",
								 loggingEvent.Level,
								 key + "=" + (loggingEvent.Properties.Contains(key) ? loggingEvent.Properties[key] : string.Empty),
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
			sut.Log(LogLevel.Trace, () => "m");
			Assert.Equal("DEBUG|m|", GetSingleMessage()); //Trace messages in log4net are rendered as Debug
		}

		[Fact]
		public void Should_be_able_to_log_trace_exception()
		{
			sut.Log(LogLevel.Trace, () => "m", new Exception("e"));
			Assert.Equal("DEBUG|m|e", GetSingleMessage()); //Trace messages in log4net are rendered as Debug
		}

		[Fact]
		public void Should_be_able_to_log_debug_message()
		{
			sut.Log(LogLevel.Debug, () => "m");
			Assert.Equal("DEBUG|m|", GetSingleMessage());
		}

		[Fact]
		public void Should_be_able_to_log_debug_exception()
		{
			sut.Log(LogLevel.Debug, () => "m", new Exception("e"));
			Assert.Equal("DEBUG|m|e", GetSingleMessage());
		}

		[Fact]
		public void Should_be_able_to_log_info_message()
		{
			sut.Log(LogLevel.Info, () => "m");
			Assert.Equal("INFO|m|", GetSingleMessage());
		}

		[Fact]
		public void Should_be_able_to_log_info_exception()
		{
			sut.Log(LogLevel.Info, () => "m", new Exception("e"));
			Assert.Equal("INFO|m|e", GetSingleMessage());
		}

		[Fact]
		public void Should_be_able_to_log_warn_message()
		{
			sut.Log(LogLevel.Warn, () => "m");
			Assert.Equal("WARN|m|", GetSingleMessage());
		}

		[Fact]
		public void Should_be_able_to_log_warn_exception()
		{
			sut.Log(LogLevel.Warn, () => "m", new Exception("e"));
			Assert.Equal("WARN|m|e", GetSingleMessage());
		}

		[Fact]
		public void Should_be_able_to_log_error_message()
		{
			sut.Log(LogLevel.Error, () => "m");
			Assert.Equal("ERROR|m|", GetSingleMessage());
		}

		[Fact]
		public void Should_be_able_to_log_error_exception()
		{
			sut.Log(LogLevel.Error, () => "m", new Exception("e"));
			Assert.Equal("ERROR|m|e", GetSingleMessage());
		}

		[Fact]
		public void Should_be_able_to_log_fatal_message()
		{
			sut.Log(LogLevel.Fatal, () => "m");
			Assert.Equal("FATAL|m|", GetSingleMessage());
		}

		[Fact]
		public void Should_be_able_to_log_fatal_exception()
		{
			sut.Log(LogLevel.Fatal, () => "m", new Exception("e"));
			Assert.Equal("FATAL|m|e", GetSingleMessage());
		}

		[Fact]
		public void Can_open_nested_context()
		{
			using (log4NetLogManager.OpenNestedConext("Context"))
			{
				sut.Log(LogLevel.Warn, () => "m");
			}
			Assert.Equal("WARN|Context|m|", GetSingleMessageWithNestedContext());
		}

		[Fact]
		public void Can_open_mapped_context()
		{
			using (log4NetLogManager.OpenMappedContext("Key", "Value"))
			{
				sut.Log(LogLevel.Warn, () => "m");
			}
			Assert.Equal("WARN|Key=Value|m|", GetSingleMessageWithMappedContext("Key"));
		}
	}
}