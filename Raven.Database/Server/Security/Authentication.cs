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
	    private static bool isEnabled;

        public static void EnableOnce()
        {
            isEnabled = true;
        }

		public static bool IsEnabled
		{
			get
			{
                if (isEnabled)
                {
                    isEnabled = false;
                    return true;
                }

			    return ValidateLicense.CurrentLicense.IsCommercial ||
			           ValidateLicense.CurrentLicense.ValidCommercialLicenseSeen;
			}
		}
	}
}