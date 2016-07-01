// -----------------------------------------------------------------------
//  <copyright file="FailoverServers.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Abstractions.Replication;

namespace Raven.Abstractions.Data
{
    public class FailoverServers
    {
        private readonly HashSet<ReplicationDestination> forDefaultDatabase = new HashSet<ReplicationDestination>();
        private readonly IDictionary<string, HashSet<ReplicationDestination>> forDatabases = new Dictionary<string, HashSet<ReplicationDestination>>();

        public ReplicationDestination[] ForDefaultDatabase
        {
            get
            {
                var result = new ReplicationDestination[forDefaultDatabase.Count];
                forDefaultDatabase.CopyTo(result);
                return result;
            }
            set { AddForDefaultDatabase(value); }
        }

        public IDictionary<string, ReplicationDestination[]> ForDatabases
        {
            set
            {
                foreach (var specificDatabaseServers in value)
                {
                    AddForDatabase(specificDatabaseServers.Key, specificDatabaseServers.Value);
                }
            }
        }

        public bool IsSetForDefaultDatabase
        {
            get { return forDefaultDatabase.Count > 0; }
        }

        public bool IsSetForDatabase(string databaseName)
        {
            return databaseName != null && forDatabases.Keys.Contains(databaseName) && forDatabases[databaseName] != null && forDatabases[databaseName].Count > 0;
        }

        public ReplicationDestination[] GetForDatabase(string databaseName)
        {
            if (forDatabases.Keys.Contains(databaseName) == false || forDatabases[databaseName] == null)
                return null;

            var result = new ReplicationDestination[forDatabases[databaseName].Count];
            forDatabases[databaseName].CopyTo(result);
            return result;
        }

        public void AddForDefaultDatabase(params ReplicationDestination[] destinations)
        {
            foreach (var dest in destinations)
            {
                forDefaultDatabase.Add(dest);
            }
        }

        public void AddForDatabase(string databaseName, params ReplicationDestination[] destinations)
        {
            if (forDatabases.Keys.Contains(databaseName) == false)
            {
                forDatabases[databaseName] = new HashSet<ReplicationDestination>();
            }

            foreach (var dest in destinations)
            {
                forDatabases[databaseName].Add(dest);
            }
        }
    }
}
