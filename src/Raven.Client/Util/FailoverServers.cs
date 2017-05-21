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
        private readonly HashSet<ReplicationNode> _forDefaultDatabase = new HashSet<ReplicationNode>();
        private readonly IDictionary<string, HashSet<ReplicationNode>> _forDatabases = new Dictionary<string, HashSet<ReplicationNode>>();

        public ReplicationNode[] ForDefaultDatabase
        {
            get
            {
                var result = new ReplicationNode[_forDefaultDatabase.Count];
                _forDefaultDatabase.CopyTo(result);
                return result;
            }
            set => AddForDefaultDatabase(value);
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

        public bool IsSetForDefaultDatabase => _forDefaultDatabase.Count > 0;

        public bool IsSetForDatabase(string databaseName)
        {
            return _forDatabases.Keys.Contains(databaseName) && _forDatabases[databaseName] != null && _forDatabases[databaseName].Count > 0;
        }

        public ReplicationNode[] GetForDatabase(string databaseName)
        {
            if (_forDatabases.Keys.Contains(databaseName) == false || _forDatabases[databaseName] == null)
                return null;

            var result = new ReplicationNode[_forDatabases[databaseName].Count];
            _forDatabases[databaseName].CopyTo(result);
            return result;
        }

        public void AddForDefaultDatabase(params ReplicationNode[] nodes)
        {
            foreach (var dest in nodes)
            {
                _forDefaultDatabase.Add(dest);
            }
        }

        public void AddForDatabase(string databaseName, params ReplicationNode[] nodes)
        {
            if (_forDatabases.Keys.Contains(databaseName) == false)
            {
                _forDatabases[databaseName] = new HashSet<ReplicationNode>();
            }

            foreach (var dest in nodes)
            {
                _forDatabases[databaseName].Add(dest);
            }
        }
    }
}
