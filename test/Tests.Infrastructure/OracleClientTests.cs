using System;
using Npgsql;
using Oracle.ManagedDataAccess.Client;

namespace Tests.Infrastructure
{
    public class OracleClientTests
    {
        public const string OracleDataSource = "DATA SOURCE=localhost:1521/orcl;";
        public static string ConnectionString = $"{OracleDataSource}USER ID=sys;password=qwerty;DBA Privilege=SYSDBA"; //todo: use a real connection string
        public static string LocalConnection = $"{ConnectionString};Pooling=false"; // have to use pooling=false, otherwise closed connections are kept IDLE.

        public static string LocalConnectionWithTimeout = $"{LocalConnection};connection timeout=3";

        public static readonly Lazy<string> OracleClientDatabaseConnection = new Lazy<string>(() =>
        {
            using (var con = new OracleConnection(LocalConnection))
            {
                con.Open();
            }

            return LocalConnection;
        });
    }
}
