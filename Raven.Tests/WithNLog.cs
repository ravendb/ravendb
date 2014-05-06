using System.Xml;
using NLog.Config;
using Raven.Database.Server;
using Raven.Tests.Common;

namespace Raven.Tests
{
	public class WithNLog : NoDisposalNeeded
	{
		static WithNLog()
		{
			if (NLog.LogManager.Configuration != null)
				return;

			HttpEndpointRegistration.RegisterHttpEndpointTarget();

            using (var stream = typeof(WithNLog).Assembly.GetManifestResourceStream("Raven.Tests.DefaultLogging.config"))
			using (var reader = XmlReader.Create(stream))
			{
				NLog.LogManager.Configuration = new XmlLoggingConfiguration(reader, "default-config");
			}
		}
	}
}