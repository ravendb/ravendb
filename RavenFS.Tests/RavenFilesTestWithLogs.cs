using System.Xml;
using NLog;
using NLog.Config;
using Raven.Database.Server;
using Raven.Tests.Helpers;

namespace RavenFS.Tests
{
    public class RavenFilesTestWithLogs : RavenFilesTestBase
	{
        static RavenFilesTestWithLogs()
        {
            if (LogManager.Configuration != null)
                return;

            HttpEndpointRegistration.RegisterHttpEndpointTarget();

            using (var stream = typeof(RavenFilesTestWithLogs).Assembly.GetManifestResourceStream("RavenFS.Tests.DefaultLogging.config"))
            using (var reader = XmlReader.Create(stream))
            {
                LogManager.Configuration = new XmlLoggingConfiguration(reader, "default-config");
            }
        }
	}
}