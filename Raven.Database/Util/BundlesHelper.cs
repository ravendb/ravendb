// -----------------------------------------------------------------------
//  <copyright file="BundlesHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Util
{
    public static class BundlesHelper
    {
        public static string ProcessActiveBundles(string activeBundles)
        {
            return activeBundles.Replace("PeriodicBackup", "PeriodicExport");
        }
    }
}
