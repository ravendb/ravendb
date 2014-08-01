// -----------------------------------------------------------------------
//  <copyright file="SettingsHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Configuration;

namespace RavenFS.Tests.Tools
{
	public static class SettingsHelper
	{
		public static bool UseFipsEncryptionAlgorithms
		{
			get
			{
				bool fips;
				bool.TryParse(ConfigurationManager.AppSettings["Raven/Encryption/FIPS"], out fips);

				return fips;
			}
		}
	}
}