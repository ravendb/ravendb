// -----------------------------------------------------------------------
//  <copyright file="ConfigurationRetrieverBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

using Raven.Abstractions.Extensions;

namespace Raven.Database.Config.Retriever
{
	public interface IConfigurationRetriever
	{
		string GetConfigurationSetting(string key, DocumentDatabase systemDatabase, DocumentDatabase localDatabase);

		string GetGlobalConfigurationDocumentKey(string key, DocumentDatabase systemDatabase, DocumentDatabase localDatabase);

		object GetConfigurationDocument(string key, DocumentDatabase systemDatabase, DocumentDatabase localDatabase);
	}

	public interface IConfigurationRetriever<TClass> : IConfigurationRetriever
	{
		new ConfigurationDocument<TClass> GetConfigurationDocument(string key, DocumentDatabase systemDatabase, DocumentDatabase localDatabase);
	}

	public abstract class ConfigurationRetrieverBase<TClass> : IConfigurationRetriever<TClass>
	{
		protected abstract TClass ApplyGlobalDocumentToLocal(TClass global, TClass local, DocumentDatabase systemDatabase, DocumentDatabase localDatabase);

		protected abstract TClass ConvertGlobalDocumentToLocal(TClass global, DocumentDatabase systemDatabase, DocumentDatabase localDatabase);

		public ConfigurationDocument<TClass> GetConfigurationDocument(string key, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			var global = systemDatabase.Documents.Get(GetGlobalConfigurationDocumentKey(key, systemDatabase, localDatabase), null);
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
				configurationDocument.Document = local.DataAsJson.JsonDeserialization<TClass>();
				return configurationDocument;
			}

			if (local == null)
			{
				configurationDocument.Document = ConvertGlobalDocumentToLocal(global.DataAsJson.JsonDeserialization<TClass>(), systemDatabase, localDatabase);
				return configurationDocument;
			}

			configurationDocument.Document = ApplyGlobalDocumentToLocal(global.DataAsJson.JsonDeserialization<TClass>(), local.DataAsJson.JsonDeserialization<TClass>(), systemDatabase, localDatabase);
			return configurationDocument;
		}

		object IConfigurationRetriever.GetConfigurationDocument(string key, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			return GetConfigurationDocument(key, systemDatabase, localDatabase);
		}

		public virtual string GetConfigurationSetting(string key, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			throw new NotSupportedException(GetType().Name + " does not support configuration settings.");
		}

		public abstract string GetGlobalConfigurationDocumentKey(string key, DocumentDatabase systemDatabase, DocumentDatabase localDatabase);
	}
}