using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.ETL;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Raven.Server.ServerWide.Context;
using SlowTests.Server.Documents.ETL.SQL;
using Xunit;

namespace SlowTests.Server.Documents
{
    public class SqlMigrationTests : RavenTestBase
    {
        private const string SqlDatabaseName = "SqlMigrationTestDatabase";

        private const string ConnectionStringName = "ConnectionStringTestName";

        private const string CreateDatabaseQuery = "USE master IF EXISTS(select * from sys.databases where name= '{0}') DROP DATABASE[{0}] CREATE DATABASE[{0}]";

        private const string DropDatabaseQuery = "IF EXISTS(select * from sys.databases where name= '{0}') ALTER DATABASE [{0}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; IF EXISTS(select * from sys.databases where name= '{0}') DROP DATABASE [{0}]";

        private static string _connectionString;

        public void Initialize(DocumentStore store)
        {
            var connectionString = new SqlConnectionString
            {   
            Name = ConnectionStringName,
            ConnectionString = SqlEtlTests.MasterDatabaseConnection.Value + $";Initial Catalog={SqlDatabaseName}"
        };

            var putConnectionStringOperation = new PutConnectionStringOperation<SqlConnectionString>(connectionString, store.Database);
            store.Admin.Server.Send(putConnectionStringOperation);

            DatabaseRecord record;
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                record = Server.ServerStore.Cluster.ReadDatabase(context, store.Database);
            }

            _connectionString = record.SqlConnectionStrings[ConnectionStringName].ConnectionString;
            
            using (var con = new SqlConnection(SqlEtlTests.MasterDatabaseConnection.Value))
            {
                con.Open();

                using (var dbCommand = con.CreateCommand())
                {
                    dbCommand.CommandText = string.Format(CreateDatabaseQuery, SqlDatabaseName);
                    dbCommand.ExecuteNonQuery();
                }
            }

            var assembly = Assembly.GetExecutingAssembly();

            using (var con = new SqlConnection(_connectionString))
            {
                con.Open();

                using (var dbCommand = con.CreateCommand())
                {
                    var textStreamReader = new StreamReader(assembly.GetManifestResourceStream("SlowTests.Data.create.sql"));
                    dbCommand.CommandText = textStreamReader.ReadToEnd();
                    dbCommand.ExecuteNonQuery();
                }

                using (var dbCommand = con.CreateCommand())
                {
                    var textStreamReader = new StreamReader(assembly.GetManifestResourceStream("SlowTests.Data.insert.sql"));
                    dbCommand.CommandText = textStreamReader.ReadToEnd();
                    dbCommand.ExecuteNonQuery();
                }
            }
        }

        [Fact]
        public void CanMigrateSqlDatabase()
        {
            using (var store = GetDocumentStore())
            {
                Initialize(store);

                var tablesToWrite = new List<SqlMigrationImportOperation.SqlMigrationTable>
                {
                    new SqlMigrationImportOperation.SqlMigrationTable("dbo.Customer")
                    {
                        EmbeddedTables = new List<SqlMigrationImportOperation.SqlMigrationTable>
                        {
                            new SqlMigrationImportOperation.SqlMigrationTable("dbo.Order")
                            {
                                EmbeddedTables = new List<SqlMigrationImportOperation.SqlMigrationTable>
                                {
                                    new SqlMigrationImportOperation.SqlMigrationTable("dbo.OrderItem")
                                    {
                                        EmbeddedTables = new List<SqlMigrationImportOperation.SqlMigrationTable>
                                        {
                                            new SqlMigrationImportOperation.SqlMigrationTable("dbo.Details")
                                        }
                                    }
                                }
                            },
                            new SqlMigrationImportOperation.SqlMigrationTable("dbo.Photo")
                        }
                    }
                };
                var operation = new SqlMigrationImportOperation(ConnectionStringName, tablesToWrite, binaryToAttachment:true, includeSchema: true, trimStrings: true, skipUnsupportedTypes: false, batchSize: 5);

                var result = store.Operations.Send(operation);

                Assert.True(result.Success);
                
                using (var session = store.OpenSession())
                {
                    var customer = session.Load<Customer>("dbo.Customer/5");

                    Assert.NotNull(customer);
                    
                    var attachmentsCount = session.Advanced.GetAttachmentNames(customer).Length;
                    Assert.Equal(attachmentsCount, 2);

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
            using (var store = GetDocumentStore())
            {
                Initialize(store);
                var tablesToWrite = new List<SqlMigrationImportOperation.SqlMigrationTable>
                {
                    new SqlMigrationImportOperation.SqlMigrationTable("dbo.Customer")
                    {
                        Query = "select * from [dbo].[Customer] where Id = 3"
                    }
                };

                var operation = new SqlMigrationImportOperation(ConnectionStringName, tablesToWrite, binaryToAttachment: true, includeSchema: true, trimStrings: true, skipUnsupportedTypes: false, batchSize: 5);

                var result = store.Operations.Send(operation);

                Assert.True(result.Success);

                using (var session = store.OpenSession())
                {
                    Assert.True(session.Advanced.Exists("dbo.Customer/3"));
                    Assert.False(session.Advanced.Exists("dbo.Customer/1"));
                }
            }
        }

        [Fact]
        public void CanMigrateWithPatch()
        {
            using (var store = GetDocumentStore())
            {
                Initialize(store);

                const string FirstName = "Name Test";

                var tablesToWrite = new List<SqlMigrationImportOperation.SqlMigrationTable>
                {
                    new SqlMigrationImportOperation.SqlMigrationTable("dbo.Customer")
                    {
                        Patch = $"this.{nameof(FirstName)} = '{FirstName}'"
                    }
                };

                var operation = new SqlMigrationImportOperation(ConnectionStringName, tablesToWrite, binaryToAttachment: true, includeSchema: true, trimStrings: true, skipUnsupportedTypes: false, batchSize: 5);

                var result = store.Operations.Send(operation);

                Assert.True(result.Success);

                using (var session = store.OpenSession())
                {
                    var customer = session.Load<Customer>("dbo.Customer/3");
                    Assert.Equal(customer.FirstName, FirstName);
                }
            }
        }

        [Fact]
        public void CorrectSkippedColumns()
        {
            using (var store = GetDocumentStore())
            {
                Initialize(store);

                var tablesToWrite = new List<SqlMigrationImportOperation.SqlMigrationTable>
                {
                    new SqlMigrationImportOperation.SqlMigrationTable("dbo.UnsupportedTable")
                };

                var operation = new SqlMigrationImportOperation(ConnectionStringName, tablesToWrite, binaryToAttachment: true, includeSchema: true, trimStrings: true, skipUnsupportedTypes: true, batchSize: 5);

                var result = store.Operations.Send(operation);

                Assert.True(result.Success);
                Assert.Equal(result.ColumnsSkipped.Length, 1);
                Assert.Equal(result.ColumnsSkipped[0], "dbo.UnsupportedTable: Node");
            }
        }

        [Fact]
        public void ReturnsCorrectErrors()
        {
            using (var store = GetDocumentStore())
            {
                Initialize(store);
                var tablesToWrite = new List<SqlMigrationImportOperation.SqlMigrationTable>
                {
                    new SqlMigrationImportOperation.SqlMigrationTable("dbo.Order")
                    {
                        EmbeddedTables = new List<SqlMigrationImportOperation.SqlMigrationTable>
                        {
                            new SqlMigrationImportOperation.SqlMigrationTable("dbo.OrderItem")
                            {
                                Property = "Foo"
                            },
                            new SqlMigrationImportOperation.SqlMigrationTable("dbo.OrderItem") // duplicate property
                            {
                                Property = "Foo"
                            },
                            new SqlMigrationImportOperation.SqlMigrationTable("dbo.OrderItem")
                            {
                                Query = "Invalid query"
                            },
                            new SqlMigrationImportOperation.SqlMigrationTable("dbo.NotExists"),
                            new SqlMigrationImportOperation.SqlMigrationTable("dbo.Product"), // cannot embed into 'Order'
                            new SqlMigrationImportOperation.SqlMigrationTable("")                            
                        }
                    },
                    new SqlMigrationImportOperation.SqlMigrationTable("dbo.Product"),
                    new SqlMigrationImportOperation.SqlMigrationTable("dbo.Product"), // duplicate table
                    new SqlMigrationImportOperation.SqlMigrationTable("dbo.Details")
                    {
                        Query = "select * from [dbo].[Details] OrDer bY Id"
                    },
                    new SqlMigrationImportOperation.SqlMigrationTable("dbo.Photo")
                    {
                        Query = "select Pic from [dbo].[Photo]" // query doesn't contain all primary keys
                    },
                    new SqlMigrationImportOperation.SqlMigrationTable("dbo.Customer")
                    {
                        EmbeddedTables = new List<SqlMigrationImportOperation.SqlMigrationTable>
                        {
                            new SqlMigrationImportOperation.SqlMigrationTable("dbo.Order"),
                            new SqlMigrationImportOperation.SqlMigrationTable("dbo.Order"), // duplicate embedded table
                            new SqlMigrationImportOperation.SqlMigrationTable("dbo.Photo")
                            {
                                Query = "select Id from [dbo].[Photo]" // query doesn't contain all referential keys
                            }
                        }
                    },
                    new SqlMigrationImportOperation.SqlMigrationTable("dbo.NoPkTable"),
                    new SqlMigrationImportOperation.SqlMigrationTable("dbo.UnsupportedTable"), // table contains unsupported type column
                    new SqlMigrationImportOperation.SqlMigrationTable("dbo.OrderItem")
                    {
                        Patch = "Invalid patch script"
                    }
                };

                var operation = new SqlMigrationImportOperation(ConnectionStringName, tablesToWrite, binaryToAttachment: true, includeSchema: true, trimStrings: true, skipUnsupportedTypes: false, batchSize: 5);

                var result = store.Operations.Send(operation);

                Assert.False(result.Success);
                Assert.Equal(result.Errors.Length, 13);
                Assert.True(result.Errors.Contains("Couldn't find table 'dbo.NotExists' in the sql database (Table name must include schema name)"));
                Assert.True(result.Errors.Contains("A table is missing a name"));
                Assert.True(result.Errors.Contains("Table 'dbo.Product' cannot embed into 'dbo.Order'"));
                Assert.True(result.Errors.Contains("Duplicate property name 'Foo'"));
                Assert.True(result.Errors.Contains("Duplicate table 'dbo.Product'"));
                Assert.True(result.Errors.Contains("Duplicate property name 'dbo.Order'"));
                Assert.True(result.Errors.Contains("Failed to read table 'dbo.OrderItem' using the given query"));
                Assert.True(result.Errors.Contains("Query for table 'dbo.Photo' must select all primary keys"));
                Assert.True(result.Errors.Contains("Table 'dbo.NoPkTable' must have at list 1 primary key"));
                Assert.True(result.Errors.Contains($"Cannot read column 'Node' in table 'dbo.UnsupportedTable'. (Unsupported type: {SqlDatabaseName}.sys.hierarchyid)"));
                Assert.True(result.Errors.Contains("Cannot patch table 'dbo.OrderItem' using the given script. Error: Esprima.ParserException: Line 1': Unexpected identifier\r\n   at Esprima.JavaScriptParser.ThrowUnexpectedToken(Token token, String message)\r\n   at Esprima.JavaScriptParser.ConsumeSemicolon()\r\n   at Esprima.JavaScriptParser.ParseLabelledStatement()\r\n   at Esprima.JavaScriptParser.ParseStatement()\r\n   at Esprima.JavaScriptParser.ParseStatementListItem()\r\n   at Esprima.JavaScriptParser.ParseFunctionSourceElements()\r\n   at Esprima.JavaScriptParser.ParseFunctionExpression()\r\n   at Esprima.JavaScriptParser.ParsePrimaryExpression()\r\n   at Esprima.JavaScriptParser.InheritCoverGrammar[T](Func`1 parseFunction)\r\n   at Esprima.JavaScriptParser.ParseLeftHandSideExpressionAllowCall()\r\n   at Esprima.JavaScriptParser.InheritCoverGrammar[T](Func`1 parseFunction)\r\n   at Esprima.JavaScriptParser.ParseUpdateExpression()\r\n   at Esprima.JavaScriptParser.ParseUnaryExpression()\r\n   at Esprima.JavaScriptParser.InheritCoverGrammar[T](Func`1 parseFunction)\r\n   at Esprima.JavaScriptParser.ParseExponentiationExpression()\r\n   at Esprima.JavaScriptParser.InheritCoverGrammar[T](Func`1 parseFunction)\r\n   at Esprima.JavaScriptParser.ParseBinaryExpression()\r\n   at Esprima.JavaScriptParser.InheritCoverGrammar[T](Func`1 parseFunction)\r\n   at Esprima.JavaScriptParser.ParseConditionalExpression()\r\n   at Esprima.JavaScriptParser.ParseAssignmentExpression()\r\n   at Esprima.JavaScriptParser.InheritCoverGrammar[T](Func`1 parseFunction)\r\n   at Esprima.JavaScriptParser.ParseGroupExpression()\r\n   at Esprima.JavaScriptParser.InheritCoverGrammar[T](Func`1 parseFunction)\r\n   at Esprima.JavaScriptParser.ParsePrimaryExpression()\r\n   at Esprima.JavaScriptParser.InheritCoverGrammar[T](Func`1 parseFunction)\r\n   at Esprima.JavaScriptParser.ParseLeftHandSideExpressionAllowCall()\r\n   at Esprima.JavaScriptParser.InheritCoverGrammar[T](Func`1 parseFunction)\r\n   at Esprima.JavaScriptParser.ParseUpdateExpression()\r\n   at Esprima.JavaScriptParser.ParseUnaryExpression()\r\n   at Esprima.JavaScriptParser.InheritCoverGrammar[T](Func`1 parseFunction)\r\n   at Esprima.JavaScriptParser.ParseExponentiationExpression()\r\n   at Esprima.JavaScriptParser.InheritCoverGrammar[T](Func`1 parseFunction)\r\n   at Esprima.JavaScriptParser.ParseBinaryExpression()\r\n   at Esprima.JavaScriptParser.InheritCoverGrammar[T](Func`1 parseFunction)\r\n   at Esprima.JavaScriptParser.ParseConditionalExpression()\r\n   at Esprima.JavaScriptParser.ParseAssignmentExpression()\r\n   at Esprima.JavaScriptParser.IsolateCoverGrammar[T](Func`1 parseFunction)\r\n   at Esprima.JavaScriptParser.ParseExpression()\r\n   at Esprima.JavaScriptParser.ParseExpressionStatement()\r\n   at Esprima.JavaScriptParser.ParseStatement()\r\n   at Esprima.JavaScriptParser.ParseStatementListItem()\r\n   at Esprima.JavaScriptParser.ParseFunctionSourceElements()\r\n   at Esprima.JavaScriptParser.ParseFunctionDeclaration(Boolean identifierIsOptional)\r\n   at Esprima.JavaScriptParser.ParseStatementListItem()\r\n   at Esprima.JavaScriptParser.ParseProgram(Boolean strict)\r\n   at Jint.Engine.Execute(String source, ParserOptions parserOptions)\r\n   at Raven.Server.SqlMigration.JsPatch..ctor(String patchScript) in C:\\Ariel\\ravendb-4.0-new-backup\\src\\Raven.Server\\SqlMigration\\JsPatch.cs:line 27\r\n   at Raven.Server.SqlMigration.SqlTable.GetJsPatch() in C:\\Ariel\\ravendb-4.0-new-backup\\src\\Raven.Server\\SqlMigration\\SqlTable.cs:line 40\r\n   at Raven.Server.SqlMigration.SqlDatabase.Validator.ValidatePatch(SqlTable table, SqlMigrationDocument document) in C:\\Ariel\\ravendb-4.0-new-backup\\src\\Raven.Server\\SqlMigration\\SqlDatabase.Validator.cs:line 141"));
                Assert.True(result.Errors.Contains("Query cannot contain an 'ORDER BY' clause (dbo.Details)"));
                Assert.True(result.Errors.Contains("Query for table 'dbo.Photo' must select all referential keys"));
            }
        }

        [Fact]
        public void CanGetTableSchema()
        {
            using (var store = GetDocumentStore())
            {
                Initialize(store);
                var operation = new SqlMigrationSchemaOperation(ConnectionStringName);

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
            using (var con = new SqlConnection(SqlEtlTests.MasterDatabaseConnection.Value))
            {
                con.Open();

                using (var dbCommand = con.CreateCommand())
                {
                    dbCommand.CommandText = string.Format(DropDatabaseQuery, SqlDatabaseName);

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
