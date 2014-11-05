using System.Xml;
using NLog;
using NLog.Config;
using Raven.Database.Server;
using Raven.Tests.Helpers;

namespace Raven.Tests.FileSystem
{
    public class RavenFilesTestWithLogs : RavenFilesTestBase
	{
        static RavenFilesTestWithLogs()
        {
            if (LogManager.Configuration != null)
                return;

            HttpEndpointRegistration.RegisterHttpEndpointTarget();

            using (var stream = typeof(RavenFilesTestWithLogs).Assembly.GetManifestResourceStream("Raven.Tests.FileSystem.DefaultLogging.config"))
            using (var reader = XmlReader.Create(stream))
            {
                LogManager.Configuration = new XmlLoggingConfiguration(reader, "default-config");
            }
        }
	}
}