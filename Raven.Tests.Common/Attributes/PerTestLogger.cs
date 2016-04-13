// -----------------------------------------------------------------------
//  <copyright file="EnsureTestCleanup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using System.Reflection;
using NLog;
using NLog.Config;
using NLog.Layouts;
using NLog.Targets;
using NLog.Targets.Wrappers;
using Xunit;

namespace Raven.Tests.Common.Attributes
{
    public class PerTestLogger : BeforeAfterTestAttribute
    {
        public const string TestsDirName = "LogsPerTest";
        public const string EnableFeatureEnvironmentVariable = "Raven_Enable_Per_Test_Logging";

        private static bool _customLoggerEnabled;
        private static LoggingConfiguration _savedConfiguration;

        public override void Before(MethodInfo methodUnderTest)
        {
            if (ShouldEnablePerTestLog())
            {
                if (_customLoggerEnabled)
                {
                    throw new InvalidOperationException("Looks like custom logging is already enabled. Parallel tests are not supported using this method.");
                }
                _customLoggerEnabled = true;
                _savedConfiguration = LogManager.Configuration;

                LogManager.Configuration = CreatePerTestConfiguration(methodUnderTest);
            }
        }

        public override void After(MethodInfo methodUnderTest)
        {
            if (_customLoggerEnabled)
            {
                try
                {
                    // dispose file target to avoid race condition in LogManager.Configuration
                    var fileTarget = LogManager.Configuration.AllTargets.OfType<FileTarget>().First();
                    fileTarget.Dispose();
                    LogManager.Configuration = _savedConfiguration;
                }
                finally
                {
                    _customLoggerEnabled = false;
                }
            }
        }

        private LoggingConfiguration CreatePerTestConfiguration(MethodInfo methodUnderTest)
        {
            var newConfig = new LoggingConfiguration();

            var fileTarget = new FileTarget
            {
                FileName = "${basedir}\\" + TestsDirName + "\\" + GenerateTestFileName(methodUnderTest),
                Layout = Layout.FromString("${longdate} ${logger} ${level} ${mdc:item=database} ${threadid} ${message} ${exception:format=tostring}"),
                Name = "DynamicFile"
            };

            // we avoid async logging here to avoid situation in which we delete log file after passed test 
            // and file will be recreated by nlog.

            newConfig.AddTarget(fileTarget);

            newConfig.LoggingRules.Add(new LoggingRule("Raven.*", LogLevel.Debug, fileTarget));

            return newConfig;
        }

        public static string GenerateTestFileName(MethodInfo methodUnderTest)
        {
            var typeName = methodUnderTest.DeclaringType.Name.Split('.').Last();
            return Path.Combine(CleanName(typeName), CleanName(methodUnderTest.Name + ".log"));
        }

        private static string CleanName(string fileName)
        {
            return Path.GetInvalidFileNameChars().Aggregate(fileName, (current, c) => current.Replace(c.ToString(), string.Empty));
        }

        public static bool ShouldEnablePerTestLog()
        {
            return Environment.GetEnvironmentVariable(EnableFeatureEnvironmentVariable) != null;
        }
    }
}
