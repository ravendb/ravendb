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
		private readonly DocumentDatabase database;

		private readonly Dictionary<string, DocumentType> documentTypes = new Dictionary<string, DocumentType>(StringComparer.OrdinalIgnoreCase)
		                                                                  {
			                                                                  {Constants.RavenReplicationDestinations, DocumentType.ReplicationDestinations}
		                                                                  };

		private readonly ReplicationConfigurationRetriever replicationConfigurationRetriever;

		public ConfigurationRetriever(DocumentDatabase systemDatabase, DocumentDatabase database)
		{
			Debug.Assert(systemDatabase.Name == null || systemDatabase.Name == Constants.SystemDatabase);

			this.database = database;

			replicationConfigurationRetriever = new ReplicationConfigurationRetriever(systemDatabase, database);
		}

		public ConfigurationDocument<TType> GetConfigurationDocument<TType>(string key)
			where TType : class
		{
			var documentType = DetectDocumentType(key);
			switch (documentType)
			{
				case DocumentType.ReplicationDestinations:
					return replicationConfigurationRetriever.GetReplicationDocument() as ConfigurationDocument<TType>;
				default:
					throw new NotSupportedException("Document type is not supported: " + documentType);
			}
		}

		public RavenJObject GetConfigurationDocumentAsJson(string key)
		{
			var documentType = DetectDocumentType(key);
			object result;
			switch (documentType)
			{
				case DocumentType.ReplicationDestinations:
					result = replicationConfigurationRetriever.GetReplicationDocument();
					break;
				default:
					throw new NotSupportedException("Document type is not supported: " + documentType);
			}

			if (result == null) 
				return null;

			return RavenJObject.FromObject(result);
		}

		private DocumentType DetectDocumentType(string key)
		{
			if (string.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");

			DocumentType documentType;
			if (documentTypes.TryGetValue(key, out documentType) == false)
				throw new NotSupportedException("Could not detect configuration type for: " + key);

			return documentType;
		}

		private enum DocumentType
		{
			ReplicationDestinations,
		}
	}
}