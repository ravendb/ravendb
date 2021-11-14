using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Npgsql;
using NpgsqlTypes;
using Raven.Client.Documents;
using Raven.Server;
using Raven.Server.Config;
using Xunit.Abstractions;

namespace SlowTests.Server.Integrations.PostgreSQL
{
    public class PostgreSqlIntegrationTestBase : RavenTestBase
    {
        protected Dictionary<string, string> EnablePostgresSqlSettings = new Dictionary<string, string>()
        {
            { RavenConfiguration.GetKey(x => x.Integrations.PostgreSql.Enabled), "true"},
            { RavenConfiguration.GetKey(x => x.Integrations.PostgreSql.Port), "0"} // a free port will be allocated so tests can run in parallel
        };

        protected const string CorrectUser = "root";
        protected const string CorrectPassword = "s3cr3t";

        public PostgreSqlIntegrationTestBase(ITestOutputHelper output) : base(output)
        {
        }

        private DataTable ExecuteSqlQuery( NpgsqlConnection connection, string query, Dictionary<string, (NpgsqlDbType, object)> namedArgs = null)
        {
            using var cmd = new NpgsqlCommand(query, connection);

            if (namedArgs != null)
            {
                foreach (var (key, val) in namedArgs)
                {
                    cmd.Parameters.AddWithValue(key, val.Item1, val.Item2);
                }
            }

            using var reader = cmd.ExecuteReader();

            var dt = new DataTable();
            dt.Load(reader);

            return dt;
        }

        protected async Task<DataTable> Act(DocumentStore store, string query, RavenServer server, bool? forceSslMode = null, Dictionary<string, (NpgsqlDbType, object)> parameters = null)
        {
            var connectionString = GetConnectionString(store, server, forceSslMode);

            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();

            var result = ExecuteSqlQuery(connection, query, parameters);

            await connection.CloseAsync();
            return result;
        }

        protected string GetConnectionString(DocumentStore store, RavenServer server, bool? forceSslMode = null)
        {
            var uri = new Uri(store.Urls.First());

            var host = server.GetListenIpAddresses(uri.Host).First().ToString();
            var database = store.Database;
            var port = server.PostgresServer.GetListenerPort();

            string connectionString;

            if (server.Certificate.Certificate == null || forceSslMode == false)
                connectionString = $"Host={host};Port={port};Database={database};Uid={CorrectUser};";
            else
                connectionString = $"Host={host};Port={port};Database={database};Uid={CorrectUser};Password={CorrectPassword};SSL Mode=Prefer;Trust Server Certificate=true";

            return connectionString;
        }
    }
}
