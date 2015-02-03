using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Config.Retriever
{
	public class ConfigurationSettings
	{
        public Dictionary<string, ConfigurationSetting> Results { get; set; }
	}

    public class ConfigurationSetting
    {
        public bool LocalExists { get; set; }

        public bool GlobalExists { get; set; }

        public string EffectiveValue { get; set; }

        public string GlobalValue { get; set; }
    }
}