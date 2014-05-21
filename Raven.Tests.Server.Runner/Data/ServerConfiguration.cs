using System;
using System.Collections.Generic;

using Raven.Database.Config;

namespace Raven.Tests.Server.Runner.Data
{
	[Serializable]
	public class ServerConfiguration
	{
		public ServerConfiguration()
		{
			Settings = new Dictionary<string, string>();
		}

		public int Port { get; set; }

		public bool RunInMemory { get; set; }

		public bool UseCommercialLicense { get; set; }

		public string ApiKeyName { get; set; }

		public string ApiKeySecret { get; set; }

		public IDictionary<string, string> Settings { get; set; }

		public bool HasApiKey { get { return !string.IsNullOrEmpty(ApiKeyName) && !string.IsNullOrEmpty(ApiKeySecret); } }

		public RavenConfiguration ConvertToRavenConfiguration()
		{
			var configuration = new RavenConfiguration
			                    {
				                    Port = Port,
				                    RunInMemory = RunInMemory,
				                    DefaultStorageTypeName = "esent"
			                    };

			foreach (var key in Settings.Keys)
			{
				configuration.Settings.Add(key, Settings[key]);
			}

			return configuration;
		}
	}
}