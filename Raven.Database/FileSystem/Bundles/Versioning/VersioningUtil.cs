// -----------------------------------------------------------------------
//  <copyright file="VersioningUtil.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using Lucene.Net.Documents;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Versioning.Data;
using Raven.Database.Bundles.Versioning.Data;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Util;

namespace Raven.Database.FileSystem.Bundles.Versioning
{
	public static class VersioningUtil
	{
		public const string RavenFileRevision = "Raven-File-Revision";
		public const string RavenFileParentRevision = "Raven-File-Parent-Revision";
		public const string RavenFileRevisionStatus = "Raven-File-Revision-Status";

		public const string DefaultConfigurationName = "Raven/Versioning/DefaultConfiguration";

		public static bool ChangesToRevisionsAllowed(this RavenFileSystem fileSystem)
		{
			var changesToRevisionsAllowed = fileSystem.Configuration.Settings[Constants.FileSystem.Versioning.ChangesToRevisionsAllowed];
			if (changesToRevisionsAllowed == null)
				return false;
			bool result;
			if (bool.TryParse(changesToRevisionsAllowed, out result) == false)
				return false;
			return result;
		}

		public static bool IsVersioningActive(this RavenFileSystem fileSystem, string filePath)
		{
			var exists = false;
			fileSystem.Storage.Batch(accessor => exists = accessor.IsVersioningActive(filePath));

			return exists;
		}

		public static bool IsVersioningActive(this IStorageActionsAccessor accessor, string filePath)
		{
			var versioningConfiguration = GetVersioningConfiguration(accessor, filePath);
			return versioningConfiguration != null && versioningConfiguration.Exclude == false;
		}

		public static FileVersioningConfiguration GetVersioningConfiguration(this IStorageActionsAccessor accessor, string filePath)
		{
			FileVersioningConfiguration fileVersioningConfiguration;
			var directoryName = RavenFileNameHelper.RavenDirectory(Path.GetDirectoryName(filePath));

			while (string.IsNullOrEmpty(directoryName) == false && directoryName != "/")
			{
				var configurationName = "Raven/Versioning/" + directoryName.TrimStart('/');

				if (TryGetDeserializedConfig(accessor, configurationName, out fileVersioningConfiguration)) 
					return fileVersioningConfiguration;

				directoryName = RavenFileNameHelper.RavenDirectory(Path.GetDirectoryName(directoryName));
			}

			if (TryGetDeserializedConfig(accessor, DefaultConfigurationName, out fileVersioningConfiguration))
				return fileVersioningConfiguration;

			return null;
		}

		private static bool TryGetDeserializedConfig(IStorageActionsAccessor accessor, string configurationName, out FileVersioningConfiguration fileVersioningConfiguration)
		{
			if (accessor.ConfigExists(configurationName) == false)
			{
				fileVersioningConfiguration = null;
				return false;
			}

			var configuration = accessor.GetConfig(configurationName);
			if (configuration == null)
			{
				fileVersioningConfiguration = null;
				return false;
			}

			fileVersioningConfiguration = configuration.JsonDeserialization<FileVersioningConfiguration>();
			return true;
		}
	}
}