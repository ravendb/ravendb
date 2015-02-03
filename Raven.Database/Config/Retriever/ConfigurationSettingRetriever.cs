// -----------------------------------------------------------------------
//  <copyright file="ConfigurationSettingRetriever.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Config.Retriever
{
	public class ConfigurationSettingRetriever : ConfigurationRetrieverBase<object>
	{
		private readonly Dictionary<string, string> keys = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
		                                                         {
			                                                         {Constants.DocsSoftLimit, Constants.DocsSoftLimit},
																	 {Constants.DocsHardLimit, Constants.DocsHardLimit},
																	 {Constants.SizeHardLimitInKB, Constants.SizeHardLimitInKB},
																	 {Constants.SizeSoftLimitInKB, Constants.SizeSoftLimitInKB}
		                                                         };

	    private readonly DocumentDatabase systemDatabase;
	    private GlobalSettingsDocument globalSettings;

	    public ConfigurationSettingRetriever(DocumentDatabase systemDatabase)
	    {
	        this.systemDatabase = systemDatabase;
	        systemDatabase.Notifications.OnDocumentChange += (database, notification, meta) =>
	        {
	            if (notification.Id != Constants.Global.GlobalSettingsDocumentKey)
	                return;
	            globalSettings = null;
	        };
	    }

	    protected override object ApplyGlobalDocumentToLocal(object global, object local, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			throw new NotSupportedException();
		}

		protected override object ConvertGlobalDocumentToLocal(object global, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			throw new NotSupportedException();
		}

		public override string GetGlobalConfigurationDocumentKey(string key, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			string globalKey;
			if (keys.TryGetValue(key, out globalKey))
				return globalKey;

			throw new NotSupportedException("Not supported key: " + key);
		}

        private void LoadGlobalSettings()
        {
            var json = systemDatabase.Documents.Get(Constants.Global.GlobalSettingsDocumentKey, null);
            globalSettings = json != null ? json.ToJson().JsonDeserialization<GlobalSettingsDocument>() : new GlobalSettingsDocument();
        }

	    public override ConfigurationSetting GetConfigurationSetting(string key, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
	    {
            if (globalSettings == null)
            {
                LoadGlobalSettings();
            }

	        string globalValue;
            var globalKey = GetGlobalConfigurationDocumentKey(key, systemDatabase, localDatabase);
            globalSettings.Settings.TryGetValue(globalKey, out globalValue);
			
            var localValue = localDatabase.Configuration.Settings[key];

		    var effectiveValue = localValue ?? globalValue;

		    return new ConfigurationSetting
		    {
		        EffectiveValue = effectiveValue,
		        GlobalValue = globalValue,
		        GlobalExists = globalValue != null,
		        LocalExists = localValue != null
		    };
		}
	}
}