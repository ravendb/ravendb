using System;
using System.Collections.Generic;
using System.Reflection;
using System.Windows;
using System.Xml.Linq;

namespace Raven.Studio.Infrastructure
{
	public static class ConfigurationManager
	{
		static ConfigurationManager()
		{
			AppSettings = new Dictionary<string, string>();
			ReadSettings();
		}

		public static Dictionary<string, string> AppSettings { get; set; }

		private static void ReadSettings()
		{
			// Get the name of the executing assemby - we are going to be looking in the root folder for
			// a file called app.config
			var assemblyName = Assembly.GetExecutingAssembly().FullName;
			assemblyName = assemblyName.Substring(0, assemblyName.IndexOf(','));
			var url = String.Format("{0};component/app.config", assemblyName);
			var configFile = Application.GetResourceStream(new Uri(url, UriKind.Relative));

			if (configFile != null && configFile.Stream != null)
			{
				var stream = configFile.Stream;
				var document = XDocument.Load(stream);

				foreach (XElement element in document.Descendants("appSettings").DescendantNodes())
				{
					AppSettings.Add(element.Attribute("key").Value, element.Attribute("value").Value);
				}
			}
		}
	}
}