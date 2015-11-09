// -----------------------------------------------------------------------
//  <copyright file="StoreValidCommercialLicenseInfo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Database.Commercial;
using Raven.Database.Config;
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
            if (database.Configuration.IsTenantDatabase)
                return;

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

            if (ValidateLicense.CurrentLicense.IsCommercial && database.Configuration.Core.AnonymousUserAccessMode == AnonymousUserAccessMode.Admin && database.Configuration.Licensing.AllowAdminAnonymousAccessForCommercialUse == false)
            {
                throw new InvalidOperationException("Your '" + RavenConfiguration.GetKey(x => x.Core.AnonymousUserAccessMode) + "' is set to '" + AnonymousUserAccessMode.Admin + 
                                                    "', which disables all user authentication on server. If you are aware of the consequences of this, please change the '" +
                                                    RavenConfiguration.GetKey(x => x.Licensing.AllowAdminAnonymousAccessForCommercialUse) +"' to 'true'.");
            }

            if (Authentication.IsEnabled == false && database.Configuration.Core.AnonymousUserAccessMode != AnonymousUserAccessMode.Admin)
            {
                throw new InvalidOperationException("Cannot set " + RavenConfiguration.GetKey(x => x.Core.AnonymousUserAccessMode) + " to '" + database.Configuration.Core.AnonymousUserAccessMode + "' without a valid license.\r\n" +
                                                    "This RavenDB instance doesn't have a license, and the only valid " + RavenConfiguration.GetKey(x => x.Core.AnonymousUserAccessMode) + " setting is: Admin\r\n" +
                                                    "Please change to " + RavenConfiguration.GetKey(x => x.Core.AnonymousUserAccessMode) + " to Admin, or install a valid license.");
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
                    {"ValidationTime", SystemTime.UtcNow}
                }, UuidType.Documents));
        }
    }
}
