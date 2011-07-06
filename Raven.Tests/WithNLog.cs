using System.Xml;
using NLog.Config;

namespace Raven.Tests
{
	public class WithNLog
	{
		static WithNLog()
		{
			if (NLog.LogManager.Configuration != null)
				return;

			using (var stream = typeof(RemoteClientTest).Assembly.GetManifestResourceStream("Raven.Tests.DefaultLogging.config"))
			using (var reader = XmlReader.Create(stream))
			{
				NLog.LogManager.Configuration = new XmlLoggingConfiguration(reader, "default-config");
			}
		}
	}
}