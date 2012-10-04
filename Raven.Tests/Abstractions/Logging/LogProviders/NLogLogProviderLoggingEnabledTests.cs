using System;
using NLog.Config;
using NLog.Targets;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Logging.LogProviders;
using Xunit;
using LogLevel = NLog.LogLevel;
using LogManager = NLog.LogManager;

namespace Raven.Tests.Abstractions.Logging.LogProviders
{
	public class NLogLogProviderLoggingEnabledTests : IDisposable
	{
		private readonly ILog sut;
		private readonly MemoryTarget target;

		public NLogLogProviderLoggingEnabledTests()
		{
			NLogLogManager.ProviderIsAvailabileOverride = true;
			var config = new LoggingConfiguration();
			target = new MemoryTarget();
			target.Layout = "${level:uppercase=true}|${message}|${exception}";
			config.AddTarget("memory", target);
			config.LoggingRules.Add(new LoggingRule("*", LogLevel.Trace, target));
			LogManager.Configuration = config;
			sut = new NLogLogManager().GetLogger("Test");
		}

		public void Dispose()
		{
			LogManager.Configuration = null;
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
			Assert.NotEmpty(target.Logs);
			Assert.Equal("TRACE|m|", target.Logs[0]);
		}

		[Fact]
		public void Should_be_able_to_log_trace_exception()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Trace, () => "m", new Exception("e"));
			Assert.NotEmpty(target.Logs);
			Assert.Equal("TRACE|m|e", target.Logs[0]);
		}

		[Fact]
		public void Should_be_able_to_log_debug_message()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Debug, () => "m");
			Assert.NotEmpty(target.Logs);
			Assert.Equal("DEBUG|m|", target.Logs[0]);
		}

		[Fact]
		public void Should_be_able_to_log_debug_exception()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Debug, () => "m", new Exception("e"));
			Assert.NotEmpty(target.Logs);
			Assert.Equal("DEBUG|m|e", target.Logs[0]);
		}

		[Fact]
		public void Should_be_able_to_log_info_message()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Info, () => "m");
			Assert.NotEmpty(target.Logs);
			Assert.Equal("INFO|m|", target.Logs[0]);
		}

		[Fact]
		public void Should_be_able_to_log_info_exception()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Info, () => "m", new Exception("e"));
			Assert.NotEmpty(target.Logs);
			Assert.Equal("INFO|m|e", target.Logs[0]);
		}

		[Fact]
		public void Should_be_able_to_log_warn_message()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Warn, () => "m");
			Assert.NotEmpty(target.Logs);
			Assert.Equal("WARN|m|", target.Logs[0]);
		}

		[Fact]
		public void Should_be_able_to_log_warn_exception()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Warn, () => "m", new Exception("e"));
			Assert.NotEmpty(target.Logs);
			Assert.Equal("WARN|m|e", target.Logs[0]);
		}

		[Fact]
		public void Should_be_able_to_log_error_message()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Error, () => "m");
			Assert.NotEmpty(target.Logs);
			Assert.Equal("ERROR|m|", target.Logs[0]);
		}

		[Fact]
		public void Should_be_able_to_log_error_exception()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Error, () => "m", new Exception("e"));
			Assert.NotEmpty(target.Logs);
			Assert.Equal("ERROR|m|e", target.Logs[0]);
		}

		[Fact]
		public void Should_be_able_to_log_fatal_message()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Fatal, () => "m");
			Assert.NotEmpty(target.Logs);
			Assert.Equal("FATAL|m|", target.Logs[0]);
		}

		[Fact]
		public void Should_be_able_to_log_fatal_exception()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Fatal, () => "m", new Exception("e"));
			Assert.NotEmpty(target.Logs);
			Assert.Equal("FATAL|m|e", target.Logs[0]);
		}
	}
}