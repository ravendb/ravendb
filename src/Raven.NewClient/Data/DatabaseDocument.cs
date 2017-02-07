//-----------------------------------------------------------------------
// <copyright file="DatabaseDocument.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;

namespace Raven.NewClient.Abstractions.Data
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

        public DatabaseDocument(string id) : this()
        {
            Id = id;
        }
    }
}
