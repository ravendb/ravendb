﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Snowflake;
using Raven.Client.Util;
using Raven.Server.SqlMigration;
using Snowflake.Data.Client;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using TestSnowflakeConnectionString = Tests.Infrastructure.ConnectionString.SnowflakeConnectionString;

namespace SlowTests.Server.Documents.ETL.Snowflake;

public class SnowflakeEtlTests: RavenTestBase
{
    public SnowflakeEtlTests(ITestOutputHelper output) : base(output)
    {
    }
    
    private readonly List<string> _dbNames = new List<string>();
    
   private const int CommandTimeout = 10; // We want to avoid timeout exception. We don't care about performance here. The query can take long time if all the outer database are working simultaneously on the same machine 
    
    protected const string defaultScript = @"
var orderData = {
    Id: id(this),
    OrderLinesCount: this.OrderLines.length,
    TotalCost: 0
};

for (var i = 0; i < this.OrderLines.length; i++) {
    var line = this.OrderLines[i];
    orderData.TotalCost += line.Cost;
    loadToOrderLines({
        OrderId: id(this),
        Qty: line.Quantity,
        Product: line.Product,
        Cost: line.Cost
    });
}

loadToOrders(orderData);
";

    protected const string defaultScriptRegions = @"
loadToRegions({
    Id: id(this),
    Name: this.Name,
    Territories: {Type: 'Array', Value: this.Territories}
});
";
    
    
    private static DisposableAction WithSnowflakeDatabase(out string connectionString, out string databaseName, out string schemaName, string dataSet, bool includeData = true)
    {
        databaseName = "snowflake_test_" + Guid.NewGuid();
        schemaName = "snowflake_test_" + Guid.NewGuid();
        var rawConnectionString = TestSnowflakeConnectionString.Instance.VerifiedConnectionString.Value;
        
        if(string.IsNullOrEmpty(rawConnectionString))
            throw new InvalidOperationException("The connection string for Snowflake db is null");
        
        connectionString = $"{rawConnectionString}DB=\"{databaseName}\";schema=\"{schemaName}\";";

        using (var connection = new SnowflakeDbConnection(rawConnectionString))
        {
            connection.Open();

            using (var dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandTimeout = CommandTimeout;
                dbCommand.CommandText = $"CREATE DATABASE \"{databaseName}\"";
                dbCommand.ExecuteNonQuery();
                dbCommand.CommandText = $"USE DATABASE \"{databaseName}\"";
                dbCommand.ExecuteNonQuery();
                dbCommand.CommandText = $"CREATE SCHEMA \"{schemaName}\"";
                dbCommand.ExecuteNonQuery();
            }
        }
        
        
        // if (string.IsNullOrEmpty(dataSet) == false)
        // {
        //     using (var dbConnection = new SnowflakeDbConnection(connectionString))
        //     {
        //         dbConnection.Open();
        //
        //         var assembly = Assembly.GetExecutingAssembly();
        //
        //         using (var dbCommand = dbConnection.CreateCommand())
        //         {
        //             dbCommand.CommandTimeout = CommandTimeout;
        //             var textStreamReader = new StreamReader(assembly.GetManifestResourceStream("SlowTests.Data.snowflake." + dataSet + ".create.sql"));
        //             dbCommand.CommandText = textStreamReader.ReadToEnd();
        //             dbCommand.ExecuteNonQuery();
        //         }
        //
        //         if (includeData)
        //         {
        //             using (var dbCommand = dbConnection.CreateCommand())
        //             {
        //                 dbCommand.CommandTimeout = CommandTimeout;
        //                 var textStreamReader = new StreamReader(assembly.GetManifestResourceStream("SlowTests.Data.mysql." + dataSet + ".insert.sql"));
        //                 dbCommand.CommandText = textStreamReader.ReadToEnd();
        //                 dbCommand.ExecuteNonQuery();
        //             }
        //         }
        //     }
        // }

        string dbName = databaseName;
        return new DisposableAction(() =>
        {
            using (var con = new SnowflakeDbConnection(rawConnectionString))
            {
                con.Open();

                using (var dbCommand = con.CreateCommand())
                {
                    dbCommand.CommandTimeout = CommandTimeout;
                    var dropDatabaseQuery = "DROP DATABASE \"{0}\"";
                    dbCommand.CommandText = string.Format(dropDatabaseQuery, dbName);

                    dbCommand.ExecuteNonQuery();
                }
            }
        });
    }
    
    private static void AssertCounts(int ordersCount, int orderLineCounts, string connectionString)
    {
        using (var con = new SnowflakeDbConnection())
        {
            con.ConnectionString = connectionString;
            con.Open();

            using (var dbCommand = con.CreateCommand())
            {
                dbCommand.CommandText = "SELECT COUNT(*) FROM Orders";
                var result = dbCommand.ExecuteScalar();
                Assert.Equal((long)ordersCount, result);
                dbCommand.CommandText = "SELECT COUNT(*) FROM OrderLines";
                result = dbCommand.ExecuteScalar();
                Assert.Equal((long)orderLineCounts, result);
            }
        }
    }
    
    private static void AssertCountsRegions(int regionsCount, string connectionString)
    {
        using (var con = new SnowflakeDbConnection())
        {
            con.ConnectionString = connectionString;
            con.Open();

            using (var dbCommand = con.CreateCommand())
            {
                dbCommand.CommandText = "SELECT COUNT(*) FROM Regions";
                var result = dbCommand.ExecuteScalar();
                Assert.Equal((long)regionsCount, result);
            }
        }
    }
    
    
    public override void Dispose()
    {
        base.Dispose();

        using (var con = new SnowflakeDbConnection())
        {
            con.ConnectionString = TestSnowflakeConnectionString.Instance.VerifiedConnectionString.Value;
            con.Open();

            foreach (var dbName in _dbNames)
            {
                using (var dbCommand = con.CreateCommand())
                {
                    dbCommand.CommandText = $@"
ALTER DATABASE SqlReplication-{dbName} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE SqlReplication-{dbName}";
                    dbCommand.ExecuteNonQuery();
                }
            }
        }
    }

    protected void SetupSnowflakeEtl(DocumentStore store, string connectionString, string script, bool insertOnly = false, List<string> collections = null)
    {
        var connectionStringName = $"{store.Database}@{store.Urls.First()} to Snowflake DB";

        Etl.AddEtl(store,
            new SnowflakeEtlConfiguration()
            {
                Name = connectionStringName,
                ConnectionStringName = connectionStringName,
                SnowflakeTables =
                {
                    new SnowflakeEtlTable { TableName = "Orders", DocumentIdColumn = "Id", InsertOnlyMode = insertOnly },
                    new SnowflakeEtlTable { TableName = "OrderLines", DocumentIdColumn = "OrderId", InsertOnlyMode = insertOnly },
                },
                Transforms = { new Transformation() { Name = "OrdersAndLines", Collections = collections ?? new List<string> { "Orders" }, Script = script } }
            }, new SnowflakeConnectionString { Name = connectionStringName, ConnectionString = connectionString, });
    }

    protected void SetupComplexDataSnowflakeEtl(DocumentStore store, string connectionString, string script, bool insertOnly = false, List<string> collections = null)
    {
        var connectionStringName = $"{store.Database}@{store.Urls.First()} to Snowflake DB";
        
        Etl.AddEtl(store,
            new SnowflakeEtlConfiguration()
            {
                Name = $"Regions{connectionStringName}",
                ConnectionStringName = connectionStringName,
                SnowflakeTables = { new SnowflakeEtlTable() { TableName = "Regions", DocumentIdColumn = "Id", InsertOnlyMode = insertOnly } },
                Transforms = { new Transformation() { Name = "Regions", Collections = new List<string>() { "Regions" }, Script = defaultScriptRegions } }
            }, new SnowflakeConnectionString() { Name = connectionStringName, ConnectionString = connectionString });
    }

    protected static void CreateOrdersAndOrderLinesTables(string connectionString)
    {
        using (var con = new SnowflakeDbConnection())
        {
            con.ConnectionString = connectionString;
            con.Open();

            using (var dbCommand = con.CreateCommand())
            {
                dbCommand.CommandText = "create or replace TABLE ORDERS (\n\tID VARCHAR(50),\n\tORDERLINESCOUNT NUMBER(38,0),\n\tTOTALCOST NUMBER(38,0),\n\tCITY VARCHAR(50)\n);";
                dbCommand.ExecuteNonQuery();
                
                dbCommand.CommandText = "create or replace TABLE ORDERLINES (\n\tID VARCHAR(50),\n\tORDERID VARCHAR(50),\n\tQTY NUMBER(38,0),\n\tPRODUCT VARCHAR(255),\n\tCOST NUMBER(38,0)\n);";
                dbCommand.ExecuteNonQuery();
            }
        }
    }

    protected static void CreateRegionsTables(string connectionString)
    {
        using (var con = new SnowflakeDbConnection())
        {
            con.ConnectionString = connectionString;
            con.Open();

            using (var dbCommand = con.CreateCommand())
            {
                dbCommand.CommandText = "create or replace TABLE REGIONS (\n\tID STRING,\n\tNAME STRING,\n\tTERRITORIES ARRAY\n);";
                dbCommand.ExecuteNonQuery();
            }
        }
    }
    
    [RequiresSnowflakeFact]
    public async Task CanUseSnowflakeSqlEtl()
    {
        using (var store = GetDocumentStore())
        {
            using (WithSnowflakeDatabase(out var connectionString, out string databaseName, out string schemaName, dataSet: null, includeData: false))
            {
                CreateOrdersAndOrderLinesTables(connectionString);
                
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        Id = "orders/1",
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine { Cost = 3, Product = "Milk", Quantity = 3 }, new OrderLine { Cost = 4, Product = "Bear", Quantity = 2 },
                        },
                    });
                    await session.SaveChangesAsync();
                }

                var etlDone = Etl.WaitForEtlToComplete(store);
                
                SetupSnowflakeEtl(store, connectionString, defaultScript, insertOnly: true);

                etlDone.Wait(TimeSpan.FromSeconds(10));

                AssertCounts(1, 2, connectionString);

                etlDone.Reset();

                using (var session = store.OpenAsyncSession())
                {
                    var order = await session.LoadAsync<Order>("orders/1");
                    order.OrderLines.Add(new OrderLine { Cost = 5, Product = "Sugar", Quantity = 7 });
                    await session.SaveChangesAsync();
                }

                etlDone.Wait(TimeSpan.FromMinutes(5));
                // we end up with duplicates
                AssertCounts(2, 5, connectionString);
            }
        }
    }
    
    [RequiresSnowflakeFact]
    public async Task CanUseSnowflakeSqlEtlForComplexData()
    {
        using (var store = GetDocumentStore())
        {
            using (WithSnowflakeDatabase(out var connectionString, out string databaseName, out string schemaName, dataSet: null, includeData: false))
            {
                CreateRegionsTables(connectionString);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Region { Name= "Special", Territories = [new Territory { Code = "95060", Name = "Santa Cruz" }] });
                    await session.SaveChangesAsync();
                }

                var etlDone = Etl.WaitForEtlToComplete(store);

                SetupComplexDataSnowflakeEtl(store, connectionString, defaultScriptRegions, insertOnly: true);

                etlDone.Wait(TimeSpan.FromSeconds(10));
                
                AssertCountsRegions(1, connectionString);
            }
        }
    }
    
    private class Order
    {
        public Address Address { get; set; }
        public string Id { get; set; }
        public List<OrderLine> OrderLines { get; set; }
    }

    private class Address
    {
        public string City { get; set; }
    }

    private class OrderLine
    {
        public string Product { get; set; }
        public int Quantity { get; set; }
        public int Cost { get; set; }
    }

    private class FavouriteOrder : Order
    {

    }

}
