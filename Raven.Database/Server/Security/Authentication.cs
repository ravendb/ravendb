// -----------------------------------------------------------------------
//  <copyright file="Authentication.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Database.Commercial;

namespace Raven.Database.Server.Security
{
	public static class Authentication
	{
		public static bool IsEnabled
		{
			get
			{
				return ValidateLicense.CurrentLicense.IsCommercial ||
				       ValidateLicense.CurrentLicense.ValidCommercialLicenseSeen;
			}
		}
	}
}