// -----------------------------------------------------------------------
//  <copyright file="ActiveBundlesProtection.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Extensions;
using Raven.Json.Linq;

namespace Raven.Database.Plugins.Builtins
{
    public class ActiveBundlesProtection : AbstractPutTrigger
    {
        private const string RavenDatabasesPrefix = "Raven/Databases/";

        public override VetoResult AllowPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
        {
            if (Database.Name != null && Database.Name != Constants.SystemDatabase)
                return VetoResult.Allowed;

            if (key.StartsWith(RavenDatabasesPrefix, StringComparison.InvariantCultureIgnoreCase) == false)
                return VetoResult.Allowed;

            var tempPermission = metadata[Constants.AllowBundlesChange];

            if (tempPermission != null)
                metadata.Remove(Constants.AllowBundlesChange); // this is a temp marker so do not persist this medatada

            var bundlesChangesAllowed = tempPermission != null &&
                                        tempPermission.Value<string>()
                                                      .Equals("true", StringComparison.InvariantCultureIgnoreCase);

            if (bundlesChangesAllowed)
                return VetoResult.Allowed;

            var existingDbDoc = Database.Documents.Get(key, transactionInformation);

            if (existingDbDoc == null)
                return VetoResult.Allowed;

            var currentDbDocument = existingDbDoc.DataAsJson.JsonDeserialization<DatabaseDocument>();

            var currentBundles = new List<string>();
            string value;
            if (currentDbDocument.Settings.TryGetValue(Constants.ActiveBundles, out value))
                currentBundles = value.GetSemicolonSeparatedValues();

            var newDbDocument = document.JsonDeserialization<DatabaseDocument>();
            var newBundles = new List<string>();
            if (newDbDocument.Settings.TryGetValue(Constants.ActiveBundles, out value))
                newBundles = value.GetSemicolonSeparatedValues();

            if (currentBundles.Count != newBundles.Count || currentBundles.TrueForAll(x => newBundles.Contains(x, StringComparer.InvariantCultureIgnoreCase)) == false)
            {
                return VetoResult.Deny(
                    "You should not change 'Raven/ActiveBundles' setting for a database. This setting should be set only once when a database is created. " +
                    "If you really need to override it you have to specify {\"" + Constants.AllowBundlesChange +
                    "\": true} in metadata of a database document every time when you send it." + Environment.NewLine +
                    "Current: " + string.Join("; ", currentBundles) + Environment.NewLine +
                    "New: " + string.Join("; ", newBundles));
            }
            
            return VetoResult.Allowed;
        }
    }
}
