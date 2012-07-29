using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.NetworkInformation;
using System.Text;
using System.Xml;
using NLog;

namespace Raven.Database.Util
{
	public static class PortUtil
	{
		private static readonly Logger logger = LogManager.GetCurrentClassLogger();

		const int DefaultPort = 8080;

		public static int GetPort(string portStr)
		{
			if (portStr == "*" || string.IsNullOrWhiteSpace(portStr))
			{
				if (File.Exists(AppDomain.CurrentDomain.BaseDirectory + "local.config"))
				{
					var doc = new XmlDocument();
					doc.Load(AppDomain.CurrentDomain.BaseDirectory + "local.config");
					if (doc.DocumentElement != null)
					{
						var stringPort = doc.DocumentElement.GetAttribute("Port");
						int localPort;

						if (int.TryParse(stringPort, out localPort))
						{
							return localPort;
						}
					}
				}
				var autoPort = FindPort();
				try
				{
					var localConfig = File.OpenWrite(AppDomain.CurrentDomain.BaseDirectory + "local.config");
					var writer = XmlWriter.Create(localConfig, new XmlWriterSettings
					{
						Indent = true,
						Encoding = Encoding.UTF8
					});
					writer.WriteStartElement("LocalConfig");
					writer.WriteAttributeString("Port", autoPort.ToString(CultureInfo.InvariantCulture));
					writer.WriteEndElement();
					writer.Close();
					localConfig.Dispose();
				}
				catch
				{
					logger.Info("Could not store selected port, next time the port could change");
				}
				if (autoPort != DefaultPort)
				{
					logger.Info("Default port {0} was not available, so using available port {1}", DefaultPort, autoPort);
				}
				return autoPort;
			}

			int port;
			if (int.TryParse(portStr, out port) == false)
				return DefaultPort;

			return port;
		}

		private static int FindPort()
		{
			var activeTcpListeners = IPGlobalProperties
				.GetIPGlobalProperties()
				.GetActiveTcpListeners();

			for (var port = DefaultPort; port < DefaultPort + 1024; port++)
			{
				var portCopy = port;
				if (activeTcpListeners.All(endPoint => endPoint.Port != portCopy))
					return port;
			}

			return DefaultPort;
		}
	}
}