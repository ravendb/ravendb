using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Reflection;
using System.Runtime.Serialization;
using FastTests;
using Raven.Client.Documents.Operations;
using Sparrow.Platform;
using Xunit;


namespace SlowTests.Server.Documents
{
    public class SqlMigrationTests : RavenTestBase
    {
        private const string DbName = "SqlMigrationTestDatabase";

        private static readonly string CreateDatabaseQuery = $"USE master IF EXISTS(select * from sys.databases where name= '{DbName}') DROP DATABASE[{DbName}] CREATE DATABASE[{DbName}]";
    
        public static string DropDatabaseQuery = $"ALTER DATABASE [{DbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{DbName}]";

        private static readonly Lazy<string> MasterDatabaseConnection = new Lazy<string>(() =>
        {
            var local = @"Data Source=localhost\sqlexpress;Integrated Security=SSPI;Connection Timeout=3";
            try
            {
                using (var con = new SqlConnection(local))
                {
                    con.Open();
                }
                return local;
            }
            catch (Exception)
            {
                try
                {
                    local = @"Data Source=ci1\sqlexpress;Integrated Security=SSPI;Connection Timeout=3";
                    using (var con = new SqlConnection(local))
                    {
                        con.Open();
                    }
                    return local;
                }
                catch
                {
                    try
                    {
                        local = @"Data Source=(localdb)\v11.0;Integrated Security=SSPI;Connection Timeout=3";
                        using (var con = new SqlConnection(local))
                        {
                            con.Open();
                        }
                        return local;
                    }
                    catch
                    {
                        try
                        {
                            string path;
                            if (PlatformDetails.RunningOnPosix)
                                path = @"/tmp/sqlReplicationPassword.txt";
                            else
                                path = @"P:\Build\SqlReplicationPassword.txt";

                            var readAllLines = File.ReadAllLines(path);
                            return $@"Data Source=ci1\sqlexpress;User Id={readAllLines[0]};Password={readAllLines[1]};Connection Timeout=1";
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("Use a valid connection", e);
                        }

                    }
                }
            }
        });

        public void Initialize()
        {
            var assembly = Assembly.GetExecutingAssembly();

            using (var con = new SqlConnection())
            {
                con.ConnectionString = MasterDatabaseConnection.Value;
                con.Open();

                using (var dbCommand = con.CreateCommand())
                {
                    dbCommand.CommandText = string.Format(CreateDatabaseQuery, DbName);
                    dbCommand.ExecuteNonQuery();
                }
            }

            using (var con = new SqlConnection())
            {
                con.ConnectionString = GetConnectionString();
                con.Open();

                using (var dbCommand = con.CreateCommand())
                {
                    var textStreamReader = new StreamReader(assembly.GetManifestResourceStream("SlowTests.Data.create.txt"));
                    dbCommand.CommandText = textStreamReader.ReadToEnd();
                    dbCommand.ExecuteNonQuery();
                }

                using (var dbCommand = con.CreateCommand())
                {
                    var textStreamReader = new StreamReader(assembly.GetManifestResourceStream("SlowTests.Data.insert.txt"));
                    dbCommand.CommandText = textStreamReader.ReadToEnd();
                    dbCommand.ExecuteNonQuery();
                }
            }
        }

        public static string GetConnectionString()
        {
            return MasterDatabaseConnection.Value + $";Initial Catalog={DbName};";
        }

        [Fact]
        public void CanMigrateSqlDatabase()
        {
            Initialize();

            using (var store = GetDocumentStore())
            {
                var tablesToWrite = new List<SqlMigrationTable>
                {
                    new SqlMigrationTable("dbo.Customer")
                    {
                        EmbeddedTables = new List<SqlMigrationTable>
                        {
                            new SqlMigrationTable("dbo.Order")
                            {
                                EmbeddedTables = new List<SqlMigrationTable>
                                {
                                    new SqlMigrationTable("dbo.OrderItem")
                                    {
                                        EmbeddedTables = new List<SqlMigrationTable>
                                        {
                                            new SqlMigrationTable("dbo.Details")
                                        }
                                    }
                                }
                            },
                            new SqlMigrationTable("dbo.Photo")
                        }
                    }
                };

                var operation = new SqlMigrationOperation(GetConnectionString(), true, true, true, false, tablesToWrite);

                var result = store.Operations.Send(operation);

                Assert.Equal(result.Success, true);

                using (var session = store.OpenSession())
                {
                    var customer = session.Load<Customer>("dbo.Customer/5");

                    Assert.NotNull(customer);

                    var attachments = session.Advanced.GetAttachmentNames(customer).Length;
                    Assert.Equal(attachments, 2);

                    Assert.Equal(customer.Photos.Count, 2);
                    Assert.Equal(customer.Orders.Count, 1);

                    var order = customer.Orders[0];
                    Assert.Equal(order.OrderItems.Count, 2);

                    foreach (var orderItem in order.OrderItems)
                        Assert.Equal(orderItem.Details.Count, 1);
                }
            }
        }

        [Fact]
        public void CanMigrateWithQuery()
        {
            Initialize();
            using (var store = GetDocumentStore())
            {
                var tablesToWrite = new List<SqlMigrationTable>
                {
                    new SqlMigrationTable("dbo.Customer")
                    {
                        Query = "select top 1 * from [dbo].[Customer]"
                    }
                };

                var operation = new SqlMigrationOperation(GetConnectionString(), true, true, true, false, tablesToWrite);

                var result = store.Operations.Send(operation);

                Assert.Equal(result.Success, true);

                using (var session = store.OpenSession())
                {
                    Assert.Equal(session.Advanced.Exists("dbo.Customer/1"), true);
                    Assert.Equal(session.Advanced.Exists("dbo.Customer/2"), false);
                }
            }
        }

        [Fact]
        public void CanMigrateWithPatch()
        {
            Initialize();
            using (var store = GetDocumentStore())
            {
                const string name = "Name Test";

                var tablesToWrite = new List<SqlMigrationTable>
                {
                    new SqlMigrationTable("dbo.Customer")
                    {
                        Patch = $"this.FirstName = '{name}'"
                    }
                };

                var operation = new SqlMigrationOperation(GetConnectionString(), true, true, true, false, tablesToWrite);

                var result = store.Operations.Send(operation);

                Assert.Equal(result.Success, true);

                using (var session = store.OpenSession())
                {
                    var customer = session.Load<Customer>("dbo.Customer/3");
                    Assert.Equal(customer.FirstName, name);
                }
            }
        }

        [Fact]
        public void ReturnsCorrectErrors()
        {
            Initialize();
            using (var store = GetDocumentStore())
            {
                var tablesToWrite = new List<SqlMigrationTable>
                {
                    new SqlMigrationTable("dbo.Order")
                    {
                        EmbeddedTables = new List<SqlMigrationTable>
                        {
                            new SqlMigrationTable("dbo.OrderItem")
                            {
                                Property = "Foo"
                            },
                            new SqlMigrationTable("dbo.OrderItem")
                            {
                                Property = "Foo"
                            },
                            new SqlMigrationTable("dbo.NotExists"),
                            new SqlMigrationTable("dbo.Product"),
                            new SqlMigrationTable("")
                        }
                    },
                    new SqlMigrationTable("dbo.Product"),
                    new SqlMigrationTable("dbo.Product"),
                    new SqlMigrationTable("dbo.Details")
                    {
                        Query = "Invalid query"
                    },
                    new SqlMigrationTable("dbo.Photo")
                    {
                        Query = "select Pic from [dbo].[Photo]"
                    },
                    new SqlMigrationTable("dbo.Customer")
                    {
                        EmbeddedTables = new List<SqlMigrationTable>
                        {
                            new SqlMigrationTable("dbo.Order"),
                            new SqlMigrationTable("dbo.Order")
                        }
                    },
                    new SqlMigrationTable("dbo.NoPkTable"),
                    new SqlMigrationTable("dbo.UnsupportedTable"),
                    new SqlMigrationTable("dbo.OrderItem")
                    {
                        Patch = "Invalid patch script"
                    }
                };

                var operation = new SqlMigrationOperation(GetConnectionString(), true, true, true, false, tablesToWrite);

                var result = store.Operations.Send(operation);

                Assert.Equal(result.Success, false);
                Assert.Equal(result.Errors.Length, 11);
                Assert.Equal(result.Errors[0], "Couldn't find table 'dbo.NotExists' in the database (Table name must include schema name)");
                Assert.Equal(result.Errors[1], "A table is missing a name");
                Assert.Equal(result.Errors[2], "Table 'dbo.Product' cannot embed into 'dbo.Order'");
                Assert.Equal(result.Errors[3], "Duplicate property name 'Foo'");
                Assert.Equal(result.Errors[4], "Duplicate table 'dbo.Product'");
                Assert.Equal(result.Errors[5], "Duplicate property name 'dbo.Order'");
                Assert.Equal(result.Errors[6], "Failed to read table 'dbo.Details' using the given query");
                Assert.Equal(result.Errors[7], "Query for table 'dbo.Photo' must select all primary keys");
                Assert.Equal(result.Errors[8], "Table 'dbo.NoPkTable' must have at list 1 primary key");
                Assert.Equal(result.Errors[9], "Cannot read column 'Node' in table 'dbo.UnsupportedTable'");
                Assert.Equal(result.Errors[10], "Cannot patch table 'dbo.OrderItem' using the given script");
            }
        }

        [Fact]
        public void CanGetTableSchema()
        {
            Initialize();
            using (var store = GetDocumentStore())
            {
                var operation = new SqlSchemaOperation(GetConnectionString());

                var result = store.Operations.Send(operation);

                Assert.Equal(result.Tables.Length, 8);
            }
        }

        public override void Dispose()
        {
            base.Dispose();

            DropDatabase();            
        }

        private void DropDatabase()
        {
            using (var con = new SqlConnection())
            {
                con.ConnectionString = MasterDatabaseConnection.Value;
                con.Open();

                using (var dbCommand = con.CreateCommand())
                {
                    dbCommand.CommandText = string.Format(DropDatabaseQuery, DbName);

                    dbCommand.ExecuteNonQuery();
                }
            }
        }

        [DataContract]
        class Customer
        {
            [DataMember(Name = "Id")]
            public string Id { get; set; }

            [DataMember(Name = "FirstName")]
            public string FirstName { get; set; }

            [DataMember(Name = "dbo.Order")]
            public List<Order> Orders { get; set; }

            [DataMember(Name = "dbo.Photo")]
            public List<Photo> Photos { get; set; }   
        }

        [DataContract]
        class Order
        {
            [DataMember(Name = "Id")]
            public string Id { get; set; }

            [DataMember(Name = "OrderDate")]
            public DateTime OrderDate { get; set; }

            [DataMember(Name = "CustomerId")]
            public string CustomerId { get; set; }

            [DataMember(Name = "TotalAmount")]
            public double TotalAmount { get; set; }

            [DataMember(Name = "dbo.OrderItem")]
            public List<OrderItem> OrderItems{ get; set; }

        }

        [DataContract]
        class OrderItem
        {
            [DataMember(Name = "OrderId")]
            public string OrderId { get; set; }

            [DataMember(Name = "ProductId")]
            public string ProductId { get; set; }

            [DataMember(Name = "UnitPrice")]
            public double UnitPrice { get; set; }

            [DataMember(Name = "dbo.Details")]
            public List<Details> Details { get; set; }
        }

        class Details
        {
            public string ID { get; set; }
            public string OrderId { get; set; }
            public string ProductId { get; set; }
            public string Name { get; set; }
        }

        class Photo
        {
            public string Id { get; set; }
            public string Photographer { get; set; }
            public string InPic1 { get; set; }
            public string InPic2 { get; set; }
        }
    }
}
