using System;
using System.IO;
using System.Text;
using FastTests;
using Raven.Server.Logging;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_27893 : RavenTestBase
    {
        public RavenDB_27893(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Logging)]
        public void StreamTargetReceivesLogEntriesAfterLoggingWasConfigured()
        {
            var logger = RavenLogManager.Instance.GetLoggerForServer<RavenDB_27893>();
            var firstMarker = Guid.NewGuid().ToString("N");
            var secondMarker = Guid.NewGuid().ToString("N");

            using (var stream = new MemoryStream())
            {
                using (StreamTarget.Register(stream))
                {
                    Assert.True(logger.IsInfoEnabled);
                    logger.Info(firstMarker);
                    logger.Info(secondMarker);
                }

                var lines = Encoding.UTF8.GetString(stream.ToArray()).Split(Environment.NewLine);

                Assert.Single(lines, line => line.Contains(firstMarker));
                Assert.Single(lines, line => line.Contains(secondMarker));
                Assert.DoesNotContain(lines, line => line.Contains(firstMarker) && line.Contains(secondMarker));
            }
        }
    }
}
