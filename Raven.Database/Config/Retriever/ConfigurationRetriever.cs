// -----------------------------------------------------------------------
//  <copyright file="ConfigurationRetriever.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;

using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Config.Retriever
{
	public class ConfigurationRetriever
	{
		private readonly Dictionary<string, DocumentType> documentTypes = new Dictionary<string, DocumentType>(StringComparer.OrdinalIgnoreCase)
		                                                                  {
			                                                                  {Constants.RavenReplicationDestinations, DocumentType.ReplicationDestinations},
																			  {Constants.Versioning.RavenVersioningDefaultConfiguration, DocumentType.VersioningConfiguration}
		                                                                  };

		private readonly ReplicationConfigurationRetriever replicationConfigurationRetriever;

		private readonly VersioningConfigurationRetriever versioningConfigurationRetriever;

		public ConfigurationRetriever(DocumentDatabase systemDatabase, DocumentDatabase database)
		{
			Debug.Assert(systemDatabase.Name == null || systemDatabase.Name == Constants.SystemDatabase);

			replicationConfigurationRetriever = new ReplicationConfigurationRetriever(systemDatabase, database);
			versioningConfigurationRetriever = new VersioningConfigurationRetriever(systemDatabase, database);
		}

		public ConfigurationDocument<TType> GetConfigurationDocument<TType>(string key)
			where TType : class
		{
			var result = GetConfigurationDocumentInternal(key);
			if (result == null)
				return null;

			return (ConfigurationDocument<TType>)result;
		}

		public RavenJObject GetConfigurationDocumentAsJson(string key)
		{
			var result = GetConfigurationDocumentInternal(key);
			if (result == null)
				return null;

			return RavenJObject.FromObject(result);
		}

		public object GetConfigurationDocumentInternal(string key)
		{
			var documentType = DetectDocumentType(key);
			switch (documentType)
			{
				case DocumentType.ReplicationDestinations:
					return replicationConfigurationRetriever.GetConfigurationDocument(key);
				case DocumentType.VersioningConfiguration:
					return versioningConfigurationRetriever.GetConfigurationDocument(key);
				default:
					throw new NotSupportedException("Document type is not supported: " + documentType);
			}
		}

		private DocumentType DetectDocumentType(string key)
		{
			if (string.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");

			DocumentType documentType;
			if (documentTypes.TryGetValue(key, out documentType) == false)
			{
				if (key.StartsWith(Constants.Versioning.RavenVersioningPrefix, StringComparison.OrdinalIgnoreCase))
					return DocumentType.VersioningConfiguration;

				throw new NotSupportedException("Could not detect configuration type for: " + key);
			}

			return documentType;
		}

		private enum DocumentType
		{
			ReplicationDestinations,
			VersioningConfiguration
		}
	}
}