// -----------------------------------------------------------------------
//  <copyright file="ConfigurationRetrieverBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Commercial;
using Raven.Json.Linq;

namespace Raven.Database.Config.Retriever
{
	public interface IConfigurationRetriever
	{
		ConfigurationSetting GetConfigurationSetting(string key, DocumentDatabase systemDatabase, DocumentDatabase localDatabase);

		string GetGlobalConfigurationDocumentKey(string key, DocumentDatabase systemDatabase, DocumentDatabase localDatabase);

		object GetConfigurationDocument(string key, DocumentDatabase systemDatabase, DocumentDatabase localDatabase);
	}

	public interface IConfigurationRetriever<TClass> : IConfigurationRetriever
		where TClass : class
	{
		new ConfigurationDocument<TClass> GetConfigurationDocument(string key, DocumentDatabase systemDatabase, DocumentDatabase localDatabase);
	}

	public abstract class ConfigurationRetrieverBase<TClass> : IConfigurationRetriever<TClass>
		where TClass : class
	{
		private readonly bool shouldDeserialize;

		protected ConfigurationRetrieverBase()
		{
			shouldDeserialize = typeof(TClass) != typeof(RavenJObject);
		}

		protected abstract TClass ApplyGlobalDocumentToLocal(TClass global, TClass local, DocumentDatabase systemDatabase, DocumentDatabase localDatabase);

		protected abstract TClass ConvertGlobalDocumentToLocal(TClass global, DocumentDatabase systemDatabase, DocumentDatabase localDatabase);

		public ConfigurationDocument<TClass> GetConfigurationDocument(string key, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			JsonDocument global = null;
			if (ConfigurationRetriever.IsGlobalConfigurationEnabled)
				global = systemDatabase.Documents.Get(GetGlobalConfigurationDocumentKey(key, systemDatabase, localDatabase), null);

			var local = localDatabase.Documents.Get(key, null);

			if (global == null && local == null)
				return null;

			var configurationDocument = new ConfigurationDocument<TClass>
			{
				GlobalExists = global != null,
				LocalExists = local != null
			};

			if (local != null)
			{
				configurationDocument.Etag = local.Etag;
				configurationDocument.Metadata = local.Metadata;
			}

			if (global == null)
			{
				configurationDocument.MergedDocument = local.DataAsJson.JsonDeserialization<TClass>();
				return configurationDocument;
			}

			configurationDocument.GlobalDocument = ConvertGlobalDocumentToLocal(Deserialize(global), systemDatabase, localDatabase);

			if (local == null)
			{
				configurationDocument.MergedDocument = configurationDocument.GlobalDocument;
				return configurationDocument;
			}

			configurationDocument.MergedDocument = ApplyGlobalDocumentToLocal(Deserialize(global), Deserialize(local), systemDatabase, localDatabase);
			return configurationDocument;
		}

		object IConfigurationRetriever.GetConfigurationDocument(string key, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			return GetConfigurationDocument(key, systemDatabase, localDatabase);
		}

		public virtual ConfigurationSetting GetConfigurationSetting(string key, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			throw new NotSupportedException(GetType().Name + " does not support configuration settings.");
		}

		public abstract string GetGlobalConfigurationDocumentKey(string key, DocumentDatabase systemDatabase, DocumentDatabase localDatabase);

		private TClass Deserialize(JsonDocument document)
		{
			if (shouldDeserialize)
				return document.DataAsJson.JsonDeserialization<TClass>();

			return document.ToJson() as TClass;
		}
	}
}