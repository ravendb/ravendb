// -----------------------------------------------------------------------
//  <copyright file="ConfigurationSettingRetriever.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;

using Raven.Abstractions.Data;

namespace Raven.Database.Config.Retriever
{
	public class ConfigurationSettingRetriever : ConfigurationRetrieverBase<object>
	{
		private readonly Dictionary<string, string> keys = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
		                                                         {
			                                                         {Constants.DocsSoftLimit, Constants.Global.QuotasDocsSoftLimitSettingKey},
																	 {Constants.DocsHardLimit, Constants.Global.QuotasDocsHardLimitSettingKey},
																	 {Constants.SizeHardLimitInKB, Constants.Global.QuotasSizeHardLimitInKBSettingKey},
																	 {Constants.SizeSoftLimitInKB, Constants.Global.QuotasSizeSoftLimitInKBSettingKey}
		                                                         };

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

		public override string GetConfigurationSetting(string key, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			var localKey = localDatabase.Configuration.Settings[key];
			if (localKey != null)
				return localKey;

			var globalKey = GetGlobalConfigurationDocumentKey(key, systemDatabase, localDatabase);
			return systemDatabase.Configuration.Settings[globalKey];
		}
	}
}