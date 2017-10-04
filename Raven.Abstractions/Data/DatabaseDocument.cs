//-----------------------------------------------------------------------
// <copyright file="DatabaseDocument.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
    public class DatabaseDocument
    {
        /// <summary>
        /// The ID of a database. Can be either the database name ("Northwind") or the full document name ("Raven/Databases/Northwind").
        /// </summary>
        public string Id { get; set; }
        
        /// <summary>
        /// Database settings (unsecured).
        /// </summary>
        public Dictionary<string, string> Settings { get; set; }

        /// <summary>
        /// Database settings (secured).
        /// </summary>
        public Dictionary<string, string> SecuredSettings { get; set; }

        /// <summary>
        /// Indicates if database is disabled or not.
        /// </summary>
        public bool Disabled { get; set; }

        public DatabaseDocument()
        {
            Settings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            SecuredSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Compares two database documents while ignoring their id.
        /// Please make sure to unprotect the document before passing it to this method.
        /// </summary>
        /// <param name="one">left hand document</param>
        /// <param name="two">right hand document</param>
        /// <returns>returns true if the two documents are equal otherwise false</returns>
        public static bool CompareDatabaseDocumentWithoutId(DatabaseDocument one, DatabaseDocument two)
        {
            if (one == null && two == null)
                return true;
            if (one == null || two == null)
                return false;
            if ( one.Disabled != two.Disabled)
                return false;
            return CompareTwoDictionaries(one.Settings, two.Settings) && CompareTwoDictionaries(one.SecuredSettings, two.SecuredSettings);
        }

        private static bool CompareTwoDictionaries(Dictionary<string, string> one, Dictionary<string, string> two)
        {
            if (one == null && two == null)
                return true;
            if (one == null || two == null)
                return false;
            return DictionaryContainsDictionary(one, two) && DictionaryContainsDictionary(two, one);
        }

        private static bool DictionaryContainsDictionary(Dictionary<string, string> one, Dictionary<string, string> two)
        {
            foreach (var keyVal in one)
            {
                string val;
                if (two.TryGetValue(keyVal.Key, out val) == false)
                    return false;
                if (val != keyVal.Value)
                    return false;
            }
            return true;
        }
    }
}
