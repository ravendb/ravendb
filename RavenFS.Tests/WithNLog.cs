using System.Xml;
using NLog;
using NLog.Config;
using Raven.Database.Server;

namespace RavenFS.Tests
{
	public class WithNLog
	{
		static WithNLog()
		{
			if (LogManager.Configuration != null)
				return;

			HttpEndpointRegistration.RegisterHttpEndpointTarget();

			using (var stream = typeof (WithNLog).Assembly.GetManifestResourceStream("RavenFS.Tests.DefaultLogging.config"))
			using (var reader = XmlReader.Create(stream))
			{
				LogManager.Configuration = new XmlLoggingConfiguration(reader, "default-config");
			}
		}
	}
}