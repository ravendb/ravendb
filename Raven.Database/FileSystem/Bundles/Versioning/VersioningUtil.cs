// -----------------------------------------------------------------------
//  <copyright file="VersioningUtil.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Versioning.Data;
using Raven.Database.FileSystem.Storage;

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

		public static bool IsVersioningActive(this IStorageActionsAccessor accessor)
		{
			return accessor.ConfigExists(DefaultConfigurationName);
		}

		public static VersioningConfiguration GetVersioningConfiguration(this IStorageActionsAccessor accessor)
		{
			if (IsVersioningActive(accessor) == false) 
				return null;

			var configuration = accessor.GetConfig(DefaultConfigurationName);
			if (configuration == null) 
				return null;

			return configuration.JsonDeserialization<VersioningConfiguration>();
		}
	}
}