// -----------------------------------------------------------------------
//  <copyright file="ConfigurationRetrieverBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Config.Retriever
{
	public abstract class ConfigurationRetrieverBase<TClass>
		where TClass : class
	{
		protected DocumentDatabase SystemDatabase { get; private set; }

		protected DocumentDatabase LocalDatabase { get; private set; }

		protected ConfigurationRetrieverBase(DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			SystemDatabase = systemDatabase;
			LocalDatabase = localDatabase;
		}

		protected abstract TClass ApplyGlobalDocumentToLocal(TClass global, TClass local);

		protected abstract TClass ConvertGlobalDocumentToLocal(TClass global);

		protected abstract string GetGlobalConfigurationDocumentKey(string key);

		public virtual ConfigurationDocument<TClass> GetConfigurationDocument(string key)
		{
			var global = SystemDatabase.Documents.Get(GetGlobalConfigurationDocumentKey(key), null);
			var local = LocalDatabase.Documents.Get(key, null);

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
				configurationDocument.Document = ConvertGlobalDocumentToLocal(global.DataAsJson.JsonDeserialization<TClass>());
				return configurationDocument;
			}

			configurationDocument.Document = ApplyGlobalDocumentToLocal(global.DataAsJson.JsonDeserialization<TClass>(), local.DataAsJson.JsonDeserialization<TClass>());
			return configurationDocument;
		}
	}
}