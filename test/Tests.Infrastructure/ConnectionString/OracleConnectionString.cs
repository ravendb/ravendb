using System;
using System.Text.RegularExpressions;
using Oracle.ManagedDataAccess.Client;

namespace Tests.Infrastructure.ConnectionString
{
    public class OracleConnectionString :SqlConnectionString<OracleConnection>
    {
        private static OracleConnectionString _instance;
        public static OracleConnectionString Instance => _instance ?? (_instance = new OracleConnectionString());

        private OracleConnectionString() : base("RAVEN_ORACLESQL_CONNECTION_STRING")
        {
            AdditionFlags = "Pooling=false"; // have to use pooling=false, otherwise closed connections are kept IDLE.
        }
        
        public string GetUserConnectionString(string newId, string newPassword)
        {
            var userConnectionString = Regex.Replace(VerifiedConnectionString.Value, "((?i:DBA Privilege)|(?i:User Id)|(?i:Password))=.+?(?:;|\\z)", (m) =>
            {
                var s = m.ToString();
                if (s.StartsWith("User Id", StringComparison.OrdinalIgnoreCase))
                {
                    return $"USER ID=\"{newId}\";";
                }

                if (s.StartsWith("Password", StringComparison.OrdinalIgnoreCase))
                {
                    return $"password={newPassword};";
                }

                return string.Empty;
            });
            return userConnectionString;
        }
    }
}
