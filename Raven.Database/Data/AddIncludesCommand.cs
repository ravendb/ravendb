//-----------------------------------------------------------------------
// <copyright file="AddIncludesCommand.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;

using Raven.Abstractions.Util;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Data
{
    public class AddIncludesCommand
    {
        public AddIncludesCommand(
            DocumentDatabase database, 
            TransactionInformation transactionInformation, 
            Action<Etag, RavenJObject, bool> add, 
            string[] includes,
            HashSet<string> loadedIds)
        {
            Add = add;
            Includes = includes;
            Database = database;
            TransactionInformation = transactionInformation;
            LoadedIds = loadedIds;
        }

        public void AlsoInclude(IEnumerable<string> ids)
        {
            foreach (var id in ids)
            {
                LoadId(id);
            }	
        }

        private string[] Includes { get; set; }

        private Action<Etag,RavenJObject, bool> Add { get; set; }

        private DocumentDatabase Database { get; set; }

        private TransactionInformation TransactionInformation { get; set; }

        private HashSet<string> LoadedIds { get; set; }


        public void Execute(RavenJObject document)
        {
            if (Includes == null)
                return;
            foreach (var include in Includes)
            {
                IncludesUtil.Include(document, include, LoadId);
            }
        }
        

        private bool LoadId(string value)
        {
            if (value == null)
                return false;

            if (LoadedIds.Add(value) == false)
                return true;

            var includedDoc = Database.Documents.Get(value, TransactionInformation);
            if (includedDoc == null)
                return false;

            Debug.Assert(includedDoc.Etag != null);
            Add(includedDoc.Etag, includedDoc.ToJson(), includedDoc.NonAuthoritativeInformation??false);

            return true;
        }
    }
}
