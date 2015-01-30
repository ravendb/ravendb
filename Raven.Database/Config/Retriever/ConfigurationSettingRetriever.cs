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

		public ConfigurationSettingRetriever(DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
			: base(systemDatabase, localDatabase)
		{
		}

		protected override object ApplyGlobalDocumentToLocal(object global, object local)
		{
			throw new NotSupportedException();
		}

		protected override object ConvertGlobalDocumentToLocal(object global)
		{
			throw new NotSupportedException();
		}

		public override string GetGlobalConfigurationDocumentKey(string key)
		{
			string globalKey;
			if (keys.TryGetValue(key, out globalKey))
				return globalKey;

			throw new NotSupportedException("Not supported key: " + key);
		}

		public override string GetConfigurationSetting(string key)
		{
			var localKey = LocalDatabase.Configuration.Settings[key];
			if (localKey != null)
				return localKey;

			var globalKey = GetGlobalConfigurationDocumentKey(key);
			return SystemDatabase.Configuration.Settings[globalKey];
		}
	}
}