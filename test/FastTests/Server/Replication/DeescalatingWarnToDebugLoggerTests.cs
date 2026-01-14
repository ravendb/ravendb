using System;
using System.Collections.Generic;
using System.Linq;
using NLog;
using NLog.Config;
using NLog.Targets;
using Raven.Server.Documents.Replication;
using Sparrow.Server.Logging;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Replication
{
    public class DeescalatingWarnToDebugLoggerTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.Logging)]
        public void FirstCall_ShouldLogAtWarnLevel()
        {
            // Arrange
            using var scope = new NLogTestSetup();
            var exception = new Exception("Test exception");
            const string message = "Test message";

            // Act
            scope.Logger.Log(message, exception);

            // Assert
            var logs = scope.GetLogs();
            Assert.Single(logs);
            Assert.Equal(LogLevel.Warn, logs[0].Level);
            Assert.Contains(message, logs[0].FormattedMessage);
        }

        [RavenFact(RavenTestCategory.Logging)]
        public void FurtherCalls_ShouldLogAtDebugLevel()
        {
            // Arrange
            using var scope = new NLogTestSetup();

            // Act
            scope.Logger.Log("Message 1", new Exception("Exception 1"));
            scope.Logger.Log("Message 2", new Exception("Exception 2"));
            scope.Logger.Log("Message 3", new Exception("Exception 3"));
            scope.Logger.Log("Message 4", new Exception("Exception 4"));

            // Assert
            var logs = scope.GetLogs();
            Assert.Equal(4, logs.Count);
            Assert.Equal(LogLevel.Warn, logs[0].Level); // Only first call
            Assert.Equal(LogLevel.Debug, logs[1].Level);
            Assert.Equal(LogLevel.Debug, logs[2].Level);
            Assert.Equal(LogLevel.Debug, logs[3].Level);
        }

        [RavenFact(RavenTestCategory.Logging)]
        public void IsEnabled_BeforeFirstCall_ShouldCheckWarnLevel()
        {
            using var scope = new NLogTestSetup(minLevel: LogLevel.Warn);

            Assert.True(scope.Logger.IsEnabled);
        }

        [RavenFact(RavenTestCategory.Logging)]
        public void Enabled_When_WarnErrorIsSet()
        {
            using var scope = new NLogTestSetup(minLevel: LogLevel.Warn); // Warn enabled, Debug disabled

            Assert.True(scope.Logger.IsEnabled, "Should be enabled before the call");

            scope.Logger.Log("Test", new Exception());

            Assert.False(scope.Logger.IsEnabled, "Should be disabled after the call");
        }

        [RavenFact(RavenTestCategory.Logging)]
        public void Log_WithWarnDisabled_ShouldNotLogFirstCall()
        {
            using var scope = new NLogTestSetup(minLevel: LogLevel.Error); // Warn disabled

            Assert.False(scope.Logger.IsEnabled, "Disabled when error level set");

            scope.Logger.Log("Message", new Exception());

            Assert.Empty(scope.GetLogs());
        }

        [RavenFact(RavenTestCategory.Logging)]
        public void Log_WithDebugDisabled_ShouldNotLogSubsequentCalls()
        {
            // Arrange
            using var scope = new NLogTestSetup(minLevel: LogLevel.Warn); // Warn enabled, Debug disabled

            // Act
            scope.Logger.Log("First", new Exception());
            scope.Logger.Log("Second", new Exception());

            // Assert
            var logs = scope.GetLogs();
            Assert.Single(logs); // Only first call logged
            Assert.Equal(LogLevel.Warn, logs[0].Level);
        }

        private class NLogTestSetup : IDisposable
        {
            private readonly LoggingConfiguration _previousConfiguration;
            private readonly TestTarget _testTarget;

            public DeescalatingWarnToDebugLogger Logger { get; }

            public NLogTestSetup(LogLevel minLevel = null)
            {
                minLevel ??= LogLevel.Debug;

                // Store previous configuration to restore later
                _previousConfiguration = LogManager.Configuration;

                // Create new configuration with test target
                var configuration = new LoggingConfiguration();
                _testTarget = new TestTarget { Name = "test" };
                configuration.AddTarget(_testTarget);

                // Add logging rule
                configuration.LoggingRules.Add(new LoggingRule("*", minLevel, _testTarget));

                LogManager.Configuration = configuration;
                var nlogLogger = LogManager.GetLogger("test");
                Logger = new DeescalatingWarnToDebugLogger(new RavenLogger(nlogLogger));
            }

            public List<LogEventInfo> GetLogs()
            {
                LogManager.Flush();
                return _testTarget.LogEvents.ToList();
            }

            public void Dispose()
            {
                LogManager.Flush();
                // Restore previous configuration
                LogManager.Configuration = _previousConfiguration;
            }
        }

        [Target("TestTarget")]
        private class TestTarget : TargetWithLayout
        {
            public List<LogEventInfo> LogEvents { get; } = new();

            protected override void Write(LogEventInfo logEvent) => LogEvents.Add(logEvent);
        }
    }
}
