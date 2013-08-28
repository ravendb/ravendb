//-----------------------------------------------------------------------
// <copyright file="RavenConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Xml.Linq;

namespace Raven.Database.Config
{
	public class RavenConfiguration : InMemoryRavenConfiguration
	{
		public RavenConfiguration()
		{
			LoadConfigurationAndInitialize(ConfigurationManager.AppSettings.AllKeys.Select(k=> Tuple.Create(k,ConfigurationManager.AppSettings[k])));
		}
		
		private void LoadConfigurationAndInitialize(IEnumerable<Tuple<string,string>> values)
		{
			foreach (var setting in values)
			{
				if (setting.Item1.StartsWith("Raven/", StringComparison.OrdinalIgnoreCase))
					Settings[setting.Item1] = setting.Item2;
			}

			Initialize();
		}


		public void LoadFrom(string path)
		{
			var configuration = XDocument.Load(path);
			if (configuration.Root == null)
				return;
			var ns = configuration.Root.Name.Namespace;
			if (configuration.Root.Name.LocalName != "configuration")
				return;

			var appSettings = configuration.Root.Element(ns + "appSettings");
			if (appSettings == null)
				return;

			var list = (from element in appSettings.Elements()
			            where element.Name.LocalName == "add"
						let keyAtt = element.Attribute(ns + "key")
						let valAtt = element.Attribute(ns + "value")
						where keyAtt != null && valAtt != null
						select Tuple.Create(keyAtt.Value, valAtt.Value)
						).ToList();


			LoadConfigurationAndInitialize(list);
		}
	}
}