using FastTests;
using Raven.Client.Documents.Operations.ETL.SQL;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10034 : NoDisposalNeeded
    {
        public RavenDB_10034(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_get_db_and_server_from_oracle_connection_string()
        {
            var (db, server) = SqlConnectionStringParser.GetDatabaseAndServerFromConnectionString("Oracle.ManagedDataAccess.Client",
                "Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.2.82)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=xe)));  User Id=system;Password=oracle;");

            Assert.Equal("xe", db);
            Assert.Equal("192.168.2.82:1521", server);

            // values are missing, should not throw
            SqlConnectionStringParser.GetDatabaseAndServerFromConnectionString("Oracle.ManagedDataAccess.Client",
                "Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(PORT=1521)));  User Id=system;Password=oracle;");


            (db, server) = SqlConnectionStringParser.GetDatabaseAndServerFromConnectionString("Oracle.ManagedDataAccess.Client",
                " Data Source=MyOracleDB;Integrated Security=yes;");

            Assert.Null(db);
            Assert.Equal("MyOracleDB", server);

            (db, server) = SqlConnectionStringParser.GetDatabaseAndServerFromConnectionString("Oracle.ManagedDataAccess.Client",
                "Data Source=username/password@myserver/myservice:dedicated/instancename;");

            Assert.Null(db);
            Assert.Equal("myserver/myservice:dedicated/instancename", server);
        }

        [Fact]
        public void Can_check_oracle_connection_string_against_secured_channel()
        {
            var c = new SqlEtlConfiguration();

            c.Connection = new SqlConnectionString()
            {
                ConnectionString = "Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)));",
                FactoryName = "Oracle.ManagedDataAccess.Client"
            };

            Assert.True(c.UsingEncryptedCommunicationChannel());

            c.Connection.ConnectionString = "Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)));";
            Assert.False(c.UsingEncryptedCommunicationChannel());

            c.Connection.ConnectionString = "Data Source=MyOracleDB;";
            Assert.False(c.UsingEncryptedCommunicationChannel());
        }
    }
}
