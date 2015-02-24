// -----------------------------------------------------------------------
//  <copyright file="GlobalSettingsDocumentProtector.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;

namespace Raven.Database.Config
{
    internal static class GlobalSettingsDocumentProtector
    {
        static readonly ILog Logger = LogManager.GetCurrentClassLogger();

        internal static void Protect(GlobalSettingsDocument settings)
        {
            if (settings.SecuredSettings == null)
            {
                settings.SecuredSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                return;
            }

            foreach (var prop in settings.SecuredSettings.ToList())
            {
                if (prop.Value == null)
                    continue;
                var bytes = Encoding.UTF8.GetBytes(prop.Value);
                var entrophy = Encoding.UTF8.GetBytes(prop.Key);
                var protectedValue = ProtectedData.Protect(bytes, entrophy, DataProtectionScope.CurrentUser);
                settings.SecuredSettings[prop.Key] = Convert.ToBase64String(protectedValue);
            }
        }

        //TODO: consider moving protect / unprotect to utils (we have 4 occurances now: db, fs, cs and here)
        internal static void Unprotect(GlobalSettingsDocument settings)
        {
            if (settings.SecuredSettings == null)
            {
                settings.SecuredSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                return;
            }

            foreach (var prop in settings.SecuredSettings.ToList())
            {
                if (prop.Value == null)
                    continue;
                var bytes = Convert.FromBase64String(prop.Value);
                var entrophy = Encoding.UTF8.GetBytes(prop.Key);
                try
                {
                    var unprotectedValue = ProtectedData.Unprotect(bytes, entrophy, DataProtectionScope.CurrentUser);
                    settings.SecuredSettings[prop.Key] = Encoding.UTF8.GetString(unprotectedValue);
                }
                catch (Exception e)
                {
                    Logger.WarnException("Could not unprotect secured global config data " + prop.Key + " setting the value to '<data could not be decrypted>'", e);
                    settings.SecuredSettings[prop.Key] = Constants.DataCouldNotBeDecrypted;
                }
            }
        }
    }
}