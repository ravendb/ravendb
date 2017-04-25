// -----------------------------------------------------------------------
//  <copyright file="FailoverServers.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using Raven.Client.Documents.Replication;

namespace Raven.Client.Util
{
    public class FailoverServers
    {
        private readonly HashSet<ReplicationNode> forDefaultDatabase = new HashSet<ReplicationNode>();
        private readonly IDictionary<string, HashSet<ReplicationNode>> forDatabases = new Dictionary<string, HashSet<ReplicationNode>>();

        public ReplicationNode[] ForDefaultDatabase
        {
            get
            {
                var result = new ReplicationNode[forDefaultDatabase.Count];
                forDefaultDatabase.CopyTo(result);
                return result;
            }
            set { AddForDefaultDatabase(value); }
        }

        public IDictionary<string, ReplicationNode[]> ForDatabases
        {
            set
            {
                foreach (var specificDatabaseServers in value)
                {
                    AddForDatabase(specificDatabaseServers.Key, specificDatabaseServers.Value);
                }
            }
        }

        public bool IsSetForDefaultDatabase => forDefaultDatabase.Count > 0;

        public bool IsSetForDatabase(string databaseName)
        {
            return forDatabases.Keys.Contains(databaseName) && forDatabases[databaseName] != null && forDatabases[databaseName].Count > 0;
        }

        public ReplicationNode[] GetForDatabase(string databaseName)
        {
            if (forDatabases.Keys.Contains(databaseName) == false || forDatabases[databaseName] == null)
                return null;

            var result = new ReplicationNode[forDatabases[databaseName].Count];
            forDatabases[databaseName].CopyTo(result);
            return result;
        }

        public void AddForDefaultDatabase(params ReplicationNode[] nodes)
        {
            foreach (var dest in nodes)
            {
                forDefaultDatabase.Add(dest);
            }
        }

        public void AddForDatabase(string databaseName, params ReplicationNode[] nodes)
        {
            if (forDatabases.Keys.Contains(databaseName) == false)
            {
                forDatabases[databaseName] = new HashSet<ReplicationNode>();
            }

            foreach (var dest in nodes)
            {
                forDatabases[databaseName].Add(dest);
            }
        }
    }
}
