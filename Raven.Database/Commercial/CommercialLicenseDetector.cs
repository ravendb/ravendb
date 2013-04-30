// -----------------------------------------------------------------------
//  <copyright file="StoreValidCommercialLicenseInfo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Database.Commercial
{
	public class CommercialLicenseDetector : IStartupTask
	{
		public void Execute(DocumentDatabase database)
		{
			if(database.Configuration.IsSystemDatabase() == false)
				return;

			if (ValidateLicense.CurrentLicense.IsCommercial && ValidateLicense.CurrentLicense.Error == false)
			{
				database.TransactionalStorage.Batch(
					accessor => accessor.Lists.Set("Raven/License/Commercial", "ValidLicense", new RavenJObject()
					{
						{"ValidationTime", DateTime.UtcNow}
					}, UuidType.Documents));

				ValidateLicense.CurrentLicense.ValidCommercialLicenseSeen = true;
			}
			else
			{
				ListItem lastSeenValidCommercialLicense = null;

				database.TransactionalStorage.Batch(accessor =>
				{
					lastSeenValidCommercialLicense = accessor.Lists.Read("Raven/License/Commercial", "ValidLicense");
				});

				if (lastSeenValidCommercialLicense != null)
				{
					ValidateLicense.CurrentLicense.ValidCommercialLicenseSeen = true;
				}
			}
		}
	}
}