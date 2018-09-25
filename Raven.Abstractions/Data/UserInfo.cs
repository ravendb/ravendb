using System.Collections.Generic;
using System.Security.Principal;

namespace Raven.Abstractions.Data
{
    public class UserInfo
    {
        /// <summary>
        /// Inforamtion about the type of user.
        /// </summary>
        public string Remark { get; set; }
        /// <summary>
        /// The specific Name of the user.
        /// </summary>
        public string User { get; set; }
        public bool IsAdminGlobal { get; set; }
        /// <summary>
        /// If using current admin db.
        /// </summary>
        public bool IsAdminCurrentDb { get; set; }
        /// <summary>
        /// A list of all the database on the server.
        /// </summary>
        public List<DatabaseInfo> Databases { get; set; }
        public IPrincipal Principal { get; set; }
        /// <summary>
        /// Return a HashSet of all the admin database on the server.
        /// </summary>
        public HashSet<string> AdminDatabases { get; set; }
        /// <summary>
        /// Return a HashSet of all the ReadOnly database on the server.
        /// </summary>
        public HashSet<string> ReadOnlyDatabases { get; set; }
        /// <summary>
        ///Return a HashSet of all the ReadWrite database on the server.
        /// </summary>
        public HashSet<string> ReadWriteDatabases { get; set; }
        /// <summary>
        /// Return the access token body.
        /// </summary>
        public AccessTokenBody AccessTokenBody { get; set; }

        public bool IsBackupOperator { get; set; }
    }

    public class DatabaseInfo
    {
        public string Database { get; set; }
        public bool IsAdmin { get; set; }
        public bool IsReadOnly { get; set; }
        
    }
}
