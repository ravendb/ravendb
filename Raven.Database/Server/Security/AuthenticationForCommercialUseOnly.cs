// -----------------------------------------------------------------------
//  <copyright file="StoreValidCommercialLicenseInfo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Database.Commercial;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Database.Server.Security
{
	public class AuthenticationForCommercialUseOnly : IStartupTask
	{
		private const string LicensingListName = "Raven/License/Commercial";
		private const string ValidationMarkerName = "ValidLicense";

		public void Execute(DocumentDatabase database)
		{
			if (database.Configuration.IsSystemDatabase())
			{
				if (ValidateLicense.CurrentLicense.IsCommercial && ValidateLicense.CurrentLicense.Error == false)
				{
					SetValidCommercialLicenseMarker(database);

					ValidateLicense.CurrentLicense.ValidCommercialLicenseSeen = true;
				}
				else
				{
					var marker = GetLastSeenValidCommercialLicenseMarker(database);

					if (marker != null)
					{
						ValidateLicense.CurrentLicense.ValidCommercialLicenseSeen = true;
					}
				}
			}

			if (Authentication.IsEnabled == false)
			{
				database.Configuration.AnonymousUserAccessMode = AnonymousUserAccessMode.Admin;
			}
		}

		private ListItem GetLastSeenValidCommercialLicenseMarker(DocumentDatabase database)
		{
			ListItem lastSeenValidCommercialLicense = null;

			database.TransactionalStorage.Batch(
				accessor => { lastSeenValidCommercialLicense = accessor.Lists.Read(LicensingListName, ValidationMarkerName); });
			return lastSeenValidCommercialLicense;
		}

		private void SetValidCommercialLicenseMarker(DocumentDatabase database)
		{
			database.TransactionalStorage.Batch(
				accessor => accessor.Lists.Set(LicensingListName, ValidationMarkerName, new RavenJObject()
				{
					{"ValidationTime", DateTime.UtcNow}
				}, UuidType.Documents));
		}
	}
}