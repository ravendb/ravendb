using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.NetworkInformation;
using System.Text;
using System.Web;
using System.Xml;
using Raven.Abstractions.Logging;

namespace Raven.Database.Util
{
	public static class PortUtil
	{
		private static readonly ILog logger = LogManager.GetCurrentClassLogger();

		const int DefaultPort = 8080;

		public static int GetPort(string portStr)
		{
			try
			{
				if (HttpContext.Current != null)
				{
					var url = HttpContext.Current.Request.Url;
					if (url.IsDefaultPort)
						return string.Equals("https", url.Scheme,StringComparison.InvariantCultureIgnoreCase) ?
						 443 : 80;
					return url.Port;
				}
			}
			catch (HttpException)
			{
				// explicitly ignoring this because we might be running in embedded mode
				// inside IIS during init stages, in which case we can't access the HttpContext
				// nor do we actually care
			}

			if (portStr == "*" || string.IsNullOrWhiteSpace(portStr))
			{
				int autoPort;

				if (TryReadPreviouslySelectAutoPort(out autoPort))
					return autoPort;

				autoPort = FindPort();
				TrySaveAutoPortForNextTime(autoPort);

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

		private static void TrySaveAutoPortForNextTime(int autoPort)
		{
			try
			{
				using (var localConfig = File.Create(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "local.config")))
				using (var writer = XmlWriter.Create(localConfig, new XmlWriterSettings
				{
					Indent = true,
					Encoding = Encoding.UTF8
				}))
				{
					writer.WriteStartElement("LocalConfig");
					writer.WriteAttributeString("Port", autoPort.ToString(CultureInfo.InvariantCulture));
					writer.WriteEndElement();
				}
			}
			catch (Exception e)
			{
				logger.InfoException("Could not store selected port to local config, next time the port could change", e);
			}
		}

		private static bool TryReadPreviouslySelectAutoPort(out int port)
		{
			port = 0;
			string localConfigPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "local.config");
			if (File.Exists(localConfigPath) == false)
			{
				return false;
			}
			var doc = new XmlDocument();
			doc.Load(localConfigPath);
			if (doc.DocumentElement == null)
				return false;

			var stringPort = doc.DocumentElement.GetAttribute("Port");
			int localPort;

			if (!int.TryParse(stringPort, out localPort))
				return false;
			
			port = localPort;
			return true;
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