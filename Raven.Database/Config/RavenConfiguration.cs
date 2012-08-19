//-----------------------------------------------------------------------
// <copyright file="RavenConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;

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
				if (setting.Item1.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase))
					Settings[setting.Item1] = setting.Item2;
			}

			Initialize();
		}


		public void LoadFrom(string path)
		{
			var configuration = ConfigurationManager.OpenExeConfiguration(path);
			LoadConfigurationAndInitialize(configuration.AppSettings.Settings.AllKeys.Select(k => Tuple.Create(k, ConfigurationManager.AppSettings[k])));
		}
	}
}