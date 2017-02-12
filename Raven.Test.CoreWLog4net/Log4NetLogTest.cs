using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Raven.Abstractions.Logging;
using Xunit;

namespace Raven.Test.CoreWLog4net
{
    public class Log4NetLogTest
    {

        [Fact]
        public void CreateLogger()
        {
            LoggerExecutionWrapper log = (LoggerExecutionWrapper)LogManager.GetLogger(typeof(Log4NetLogTest));
            Assert.NotNull(log);
            var lewType = typeof(LoggerExecutionWrapper);
            var loggerField = lewType.GetField("logger", BindingFlags.NonPublic | BindingFlags.Instance);
            var internalLogger = loggerField.GetValue(log);
            Assert.NotNull(internalLogger);
            Assert.False(internalLogger is LogManager.NoOpLogger);
            Assert.True(internalLogger is Raven.Abstractions.Logging.LogProviders.Log4NetLogManager.Log4NetLogger);

        }
    }
}
