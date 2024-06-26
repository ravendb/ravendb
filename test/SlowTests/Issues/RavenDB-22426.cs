using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.SqlClient;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.SqlMigration;
using SlowTests.Server.Documents.ETL;
using SlowTests.Server.Documents.Migration;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22426 : EtlTestBase
{
    private const string EtlScript = @"
        var updatedDto = {
            Id: id(this),
            Name: 'NewName'
        };

        loadToDtos(updatedDto);
    ";
    
    public RavenDB_22426(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void TestOldFactoryNameWithNoEncryptParameter()
    {
        using (var store = GetDocumentStore())
        {
            using (SqlAwareTestBase.WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
            {
                CreateRdbmsSchema(connectionString);

                using (var session = store.OpenSession())
                {
                    var dto = new Dto() { Name = "CoolName" };
                    
                    session.Store(dto);
                    
                    session.SaveChanges();
                }

                var etlDone = WaitForEtl(store, (n, s) => GetDtosCount(connectionString) == 1);
                
                var connectionStringWithoutEncryptOption = connectionString.Replace("Encrypt=Optional;", string.Empty);
                
                SetupSqlEtl(store, connectionStringWithoutEncryptOption, EtlScript);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                Assert.Equal(1, GetDtosCount(connectionString));
            }
        }
    }
    
    protected void CreateRdbmsSchema(string connectionString, string command = @"
CREATE TABLE [dbo].[Dtos]
(
    [Id] [nvarchar](50) NOT NULL,
    [Name] [nvarchar](50) NULL
)
")
    {
        using (var con = new SqlConnection())
        {
            con.ConnectionString = connectionString;
            con.Open();

            using (var dbCommand = con.CreateCommand())
            {
                dbCommand.CommandText = command;
                dbCommand.ExecuteNonQuery();
            }
            con.Close();
        }
    }
    
    protected static int GetDtosCount(string connectionString)
    {
        using (var con = new SqlConnection())
        {
            con.ConnectionString = connectionString;
            con.Open();

            using (var dbCommand = con.CreateCommand())
            {
                dbCommand.CommandText = " SELECT COUNT(*) FROM Dtos";
                return (int)dbCommand.ExecuteScalar();
            }
        }
    }
    
    protected void SetupSqlEtl(DocumentStore store, string connectionString, string script, bool insertOnly = false, List<string> collections = null)
    {
        var connectionStringName = $"{store.Database}@{store.Urls.First()} to SQL DB";

        AddEtl(store, new SqlEtlConfiguration()
        {
            Name = connectionStringName,
            ConnectionStringName = connectionStringName,
            SqlTables =
            {
                new SqlEtlTable {TableName = "Dtos", DocumentIdColumn = "Id", InsertOnlyMode = insertOnly}
            },
            Transforms =
            {
                new Transformation()
                {
                    Name = "UpdateName",
                    Collections = collections ?? new List<string> { "Dtos" },
                    Script = script
                }
            }
        }, new SqlConnectionString
        {
            Name = connectionStringName,
            ConnectionString = connectionString,
            FactoryName = "System.Data.SqlClient"
        });
    }

    private class Dto
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}
