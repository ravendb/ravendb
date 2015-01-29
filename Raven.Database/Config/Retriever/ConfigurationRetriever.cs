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
		private readonly DocumentDatabase systemDatabase;

		private readonly DocumentDatabase database;

		private readonly Dictionary<string, DocumentType> documentTypes = new Dictionary<string, DocumentType>(StringComparer.OrdinalIgnoreCase)
		                                                                  {
			                                                                  {Constants.RavenReplicationDestinations, DocumentType.ReplicationDestinations},
																			  {Constants.Versioning.RavenVersioningDefaultConfiguration, DocumentType.VersioningConfiguration},
																			  {PeriodicExportSetup.RavenDocumentKey, DocumentType.PeriodicExportConfiguration}
		                                                                  };

		private readonly ReplicationConfigurationRetriever replicationConfigurationRetriever;

		private readonly VersioningConfigurationRetriever versioningConfigurationRetriever;

		private readonly PeriodicExportConfigurationRetriever periodicExportConfigurationRetriever;

		public ConfigurationRetriever(DocumentDatabase systemDatabase, DocumentDatabase database)
		{
			Debug.Assert(systemDatabase.Name == null || systemDatabase.Name == Constants.SystemDatabase);

			this.systemDatabase = systemDatabase;
			this.database = database;

			replicationConfigurationRetriever = new ReplicationConfigurationRetriever(systemDatabase, database);
			versioningConfigurationRetriever = new VersioningConfigurationRetriever(systemDatabase, database);
			periodicExportConfigurationRetriever = new PeriodicExportConfigurationRetriever(systemDatabase, database);
		}

		public ConfigurationDocument<TType> GetConfigurationDocument<TType>(string key)
			where TType : class
		{
			return GetConfigurationDocumentInternal<TType>(key);
		}

		public RavenJObject GetConfigurationDocumentAsJson(string key)
		{
			var result = GetConfigurationDocumentInternal(key);
			if (result == null)
				return null;

			return RavenJObject.FromObject(result);
		}

		public void SubscribeToConfigurationChanges(string key, Action action)
		{
			var globalKey = GetGlobalConfigurationDocumentKey(key);

			systemDatabase.Notifications.OnDocumentChange += (documentDatabase, notification, metadata) => SendNotification(notification, globalKey, action);
			database.Notifications.OnDocumentChange += (documentDatabase, notification, metadata) => SendNotification(notification, key, action);
		}

		private object GetConfigurationDocumentInternal(string key)
		{
			return GetConfigurationRetriever(key).GetConfigurationDocument(key);
		}

		private ConfigurationDocument<TType> GetConfigurationDocumentInternal<TType>(string key)
		{
			return ((IConfigurationRetriever<TType>)GetConfigurationRetriever(key)).GetConfigurationDocument(key);
		}

		private IConfigurationRetriever GetConfigurationRetriever(string key)
		{
			var documentType = DetectDocumentType(key);
			switch (documentType)
			{
				case DocumentType.ReplicationDestinations:
					return replicationConfigurationRetriever;
				case DocumentType.VersioningConfiguration:
					return versioningConfigurationRetriever;
				case DocumentType.PeriodicExportConfiguration:
					return periodicExportConfigurationRetriever;
				default:
					throw new NotSupportedException("Document type is not supported: " + documentType);
			}
		}

		private string GetGlobalConfigurationDocumentKey(string key)
		{
			return GetConfigurationRetriever(key).GetGlobalConfigurationDocumentKey(key);
		}

		private static void SendNotification(DocumentChangeNotification notification, string key, Action action)
		{
			if (notification.Id == null)
				return;

			if (string.Equals(key, notification.Id, StringComparison.OrdinalIgnoreCase))
				action();
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
			VersioningConfiguration,
			PeriodicExportConfiguration
		}
	}
}