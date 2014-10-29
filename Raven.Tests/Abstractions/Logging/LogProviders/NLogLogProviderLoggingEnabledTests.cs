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
		private readonly MemoryTarget mdcLayoutTarget;
		private readonly NLogLogManager nLogLogManager;
		private readonly MemoryTarget ndcLayoutTarget;
		private readonly MemoryTarget simpleLayoutTarget;
		private readonly ILog sut;

		public NLogLogProviderLoggingEnabledTests()
		{
			NLogLogManager.ProviderIsAvailableOverride = true;
			var config = new LoggingConfiguration();

			simpleLayoutTarget = new MemoryTarget
			{
				Layout = "${level:uppercase=true}|${message}|${exception}"
			};
			ndcLayoutTarget = new MemoryTarget
			{
				Layout = "${level:uppercase=true}|${ndc:bottomFrames=10:topFrames=10:separator=;}|${message}|${exception}"
			};
			mdcLayoutTarget = new MemoryTarget
			{
				Layout = "${level:uppercase=true}|${mdc:item=Key}|${message}|${exception}"
			};
			config.AddTarget("simpleLayoutMemory", simpleLayoutTarget);
			config.AddTarget("mdcLayoutTarget", mdcLayoutTarget);
			config.AddTarget("ndcLayoutMemory", ndcLayoutTarget);
			config.LoggingRules.Add(new LoggingRule("*", LogLevel.Trace, simpleLayoutTarget));
			config.LoggingRules.Add(new LoggingRule("*", LogLevel.Trace, mdcLayoutTarget));
			config.LoggingRules.Add(new LoggingRule("*", LogLevel.Trace, ndcLayoutTarget));
			LogManager.Configuration = config;
			nLogLogManager = new NLogLogManager();
			sut = nLogLogManager.GetLogger("Test");
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
			Assert.Equal("TRACE|m|", simpleLayoutTarget.Logs[0]);
		}

		[Fact]
		public void Should_be_able_to_log_trace_exception()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Trace, () => "m", new Exception("e"));
			Assert.Equal("TRACE|m|e", simpleLayoutTarget.Logs[0]);
		}

		[Fact]
		public void Should_be_able_to_log_debug_message()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Debug, () => "m");
			Assert.Equal("DEBUG|m|", simpleLayoutTarget.Logs[0]);
		}

		[Fact]
		public void Should_be_able_to_log_debug_exception()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Debug, () => "m", new Exception("e"));
			Assert.Equal("DEBUG|m|e", simpleLayoutTarget.Logs[0]);
		}

		[Fact]
		public void Should_be_able_to_log_info_message()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Info, () => "m");
			Assert.Equal("INFO|m|", simpleLayoutTarget.Logs[0]);
		}

		[Fact]
		public void Should_be_able_to_log_info_exception()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Info, () => "m", new Exception("e"));
			Assert.Equal("INFO|m|e", simpleLayoutTarget.Logs[0]);
		}

		[Fact]
		public void Should_be_able_to_log_warn_message()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Warn, () => "m");
			Assert.Equal("WARN|m|", simpleLayoutTarget.Logs[0]);
		}

		[Fact]
		public void Should_be_able_to_log_warn_exception()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Warn, () => "m", new Exception("e"));
			Assert.Equal("WARN|m|e", simpleLayoutTarget.Logs[0]);
		}

		[Fact]
		public void Should_be_able_to_log_error_message()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Error, () => "m");
			Assert.Equal("ERROR|m|", simpleLayoutTarget.Logs[0]);
		}

		[Fact]
		public void Should_be_able_to_log_error_exception()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Error, () => "m", new Exception("e"));
			Assert.Equal("ERROR|m|e", simpleLayoutTarget.Logs[0]);
		}

		[Fact]
		public void Should_be_able_to_log_fatal_message()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Fatal, () => "m");
			Assert.Equal("FATAL|m|", simpleLayoutTarget.Logs[0]);
		}

		[Fact]
		public void Should_be_able_to_log_fatal_exception()
		{
			sut.Log(Raven.Abstractions.Logging.LogLevel.Fatal, () => "m", new Exception("e"));
			Assert.Equal("FATAL|m|e", simpleLayoutTarget.Logs[0]);
		}

		[Fact]
		public void Can_open_nested_context()
		{
			using (nLogLogManager.OpenNestedConext("Context"))
			{
				sut.Log(Raven.Abstractions.Logging.LogLevel.Warn, () => "m");
			}
			Assert.Equal("WARN|Context|m|", ndcLayoutTarget.Logs[0]);
		}

		[Fact]
		public void Can_open_mapped_context()
		{
			using (nLogLogManager.OpenMappedContext("Key", "Value"))
			{
				sut.Log(Raven.Abstractions.Logging.LogLevel.Warn, () => "m");
			}
			Assert.Equal("WARN|Value|m|", mdcLayoutTarget.Logs[0]);
		}
	}
}