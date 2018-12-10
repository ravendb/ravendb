using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Operations;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Raven.Server.SqlMigration.Model;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.SqlMigration
{
    public class BasicMigrationTests : SqlAwareTestBase
    {
        [Theory]
        [InlineData(MigrationProvider.MsSQL)]
        [RequiresNpgSqlInlineData]
        [RequiresOracleSqlInlineData]
        [RequiresMySqlInlineData]
        public async Task CanMigrateSkipOnParent(MigrationProvider provider)
        {
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, "basic"))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(provider, connectionString);
                using (var store = GetDocumentStore())
                {
                    var collection = new RootCollection(schemaName, "order", "Orders");

                    var db = await GetDocumentDatabaseInstanceFor(store);

                    var settings = new MigrationSettings
                    {
                        Collections = new List<RootCollection>
                        {
                            collection
                        }
                    };

                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var schema = driver.FindSchema();
                        ApplyDefaultColumnNamesMapping(schema, settings);
                        await driver.Migrate(settings, schema, db, context);
                    }

                    using (var session = store.OpenSession())
                    {
                        var order = session.Load<JObject>("Orders/1");

                        Assert.NotNull(order);
                        // total and metadata, Id (Orders/1)
                        Assert.Equal(3, order.Properties().Count());

                        Assert.Equal("Orders", order["@metadata"]["@collection"]);
                        Assert.Equal(440, order["Total"]);
                        Assert.Equal("Orders/1", order["Id"]);
                    }

                    var collectionStatistics = store.Maintenance.Send(new GetCollectionStatisticsOperation());
                    Assert.Equal(1, collectionStatistics.CountOfDocuments);
                }
            }
        }

        [Theory]
        [InlineData(MigrationProvider.MsSQL)]
        [RequiresNpgSqlInlineData]
        [RequiresOracleSqlInlineData]
        [RequiresMySqlInlineData]
        public async Task CanMigrateEmbedOnParent(MigrationProvider provider)
        {
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, "basic"))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(provider, connectionString);
                using (var store = GetDocumentStore())
                {
                    var collection = new RootCollection(schemaName, "order", "Orders")
                    {
                        NestedCollections = new List<EmbeddedCollection>
                        {
                            new EmbeddedCollection(schemaName, "order_item", RelationType.OneToMany, new List<string> { "order_id" }, "Items")
                        }
                    };

                    var db = await GetDocumentDatabaseInstanceFor(store);

                    var settings = new MigrationSettings
                    {
                        Collections = new List<RootCollection>
                        {
                            collection
                        }
                    };

                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var schema = driver.FindSchema();
                        ApplyDefaultColumnNamesMapping(schema, settings);
                        await driver.Migrate(settings, schema, db, context);
                    }

                    using (var session = store.OpenSession())
                    {
                        var order = session.Load<JObject>("Orders/1");

                        Assert.NotNull(order);
                        // total and metadata, Id (Orders/1), Items
                        Assert.Equal(4, order.Properties().Count());

                        Assert.Equal("Orders", order["@metadata"]["@collection"]);
                        Assert.Equal(440, order["Total"]);
                        Assert.Equal("Orders/1", order["Id"]);
                        var firstItem = order["Items"][0];
                        Assert.Equal(110, firstItem["Price"]);
                        Assert.Equal(1, firstItem.Count());

                        var secondItem = order["Items"][1];
                        Assert.Equal(330, secondItem["Price"]);
                        Assert.Equal(1, secondItem.Count());
                    }

                    var collectionStatistics = store.Maintenance.Send(new GetCollectionStatisticsOperation());
                    Assert.Equal(1, collectionStatistics.CountOfDocuments);
                }
            }
        }

        [Theory]
        [InlineData(MigrationProvider.MsSQL)]
        [RequiresNpgSqlInlineData]
        [RequiresOracleSqlInlineData]
        [RequiresMySqlInlineData]
        public async Task CanMigrateLinkOnParent(MigrationProvider provider)
        {
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, "basic"))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(provider, connectionString);
                using (var store = GetDocumentStore())
                {
                    var ordersCollection = new RootCollection(schemaName, "order", "Orders")
                    {
                        LinkedCollections = new List<LinkedCollection>
                        {
                            new LinkedCollection(schemaName, "order_item", RelationType.OneToMany, new List<string> { "order_id" }, "Items")
                        }
                    };

                    var orderItemsCollection = new RootCollection(schemaName, "order_item", "OrderItems");

                    var db = await GetDocumentDatabaseInstanceFor(store);

                    var settings = new MigrationSettings
                    {
                        Collections = new List<RootCollection>
                        {
                            ordersCollection,
                            orderItemsCollection
                        }
                    };

                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var schema = driver.FindSchema();
                        ApplyDefaultColumnNamesMapping(schema, settings);
                        await driver.Migrate(settings, schema, db, context);
                    }

                    using (var session = store.OpenSession())
                    {
                        var order = session.Load<JObject>("Orders/1");

                        Assert.NotNull(order);
                        // total and metadata, Id (Orders/1), Items
                        Assert.Equal(4, order.Properties().Count());

                        Assert.Equal("Orders", order["@metadata"]["@collection"]);
                        Assert.Equal(440, order["Total"]);
                        Assert.Equal("Orders/1", order["Id"]);
                        Assert.Equal("OrderItems/10", order["Items"][0]);
                        Assert.Equal("OrderItems/11", order["Items"][1]);
                    }

                    var collectionStatistics = store.Maintenance.Send(new GetCollectionStatisticsOperation());
                    Assert.Equal(3, collectionStatistics.CountOfDocuments);
                }
            }
        }

        [Theory]
        [InlineData(MigrationProvider.MsSQL)]
        [RequiresNpgSqlInlineData]
        [RequiresOracleSqlInlineData]
        [RequiresMySqlInlineData]
        public async Task CanMigrateSkipOnChild(MigrationProvider provider)
        {
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, "basic"))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(provider, connectionString);
                using (var store = GetDocumentStore())
                {
                    var collection = new RootCollection(schemaName, "order_item", "OrderItems");

                    var db = await GetDocumentDatabaseInstanceFor(store);

                    var settings = new MigrationSettings
                    {
                        Collections = new List<RootCollection>
                        {
                            collection
                        }
                    };

                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var schema = driver.FindSchema();
                        ApplyDefaultColumnNamesMapping(schema, settings);
                        await driver.Migrate(settings, schema, db, context);
                    }

                    using (var session = store.OpenSession())
                    {
                        var orderItem = session.Load<JObject>("OrderItems/10");

                        Assert.NotNull(orderItem);
                        // price and metadata, Id (OrderItems/1)
                        Assert.Equal(3, orderItem.Properties().Count());

                        Assert.Equal("OrderItems", orderItem["@metadata"]["@collection"]);
                        Assert.Equal(110, orderItem["Price"]);
                        Assert.Equal("OrderItems/10", orderItem["Id"]);
                    }

                    var collectionStatistics = store.Maintenance.Send(new GetCollectionStatisticsOperation());
                    Assert.Equal(2, collectionStatistics.CountOfDocuments);
                }
            }
        }

        [Theory]
        [InlineData(MigrationProvider.MsSQL)]
        [RequiresNpgSqlInlineData]
        [RequiresOracleSqlInlineData]
        [RequiresMySqlInlineData]
        public async Task CanMigrateEmbedOnChild(MigrationProvider provider)
        {
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, "basic"))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(provider, connectionString);
                using (var store = GetDocumentStore())
                {
                    var collection = new RootCollection(schemaName, "order_item", "OrderItems")
                    {
                        NestedCollections = new List<EmbeddedCollection>
                        {
                            new EmbeddedCollection(schemaName, "order", RelationType.ManyToOne, new List<string> { "order_id" }, "Order")
                        }
                    };

                    var db = await GetDocumentDatabaseInstanceFor(store);

                    var settings = new MigrationSettings
                    {
                        Collections = new List<RootCollection>
                        {
                            collection
                        }
                    };

                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var schema = driver.FindSchema();
                        ApplyDefaultColumnNamesMapping(schema, settings);
                        await driver.Migrate(settings, schema, db, context);
                    }

                    using (var session = store.OpenSession())
                    {
                        var orderItem = session.Load<JObject>("OrderItems/10");

                        Assert.NotNull(orderItem);
                        // price and metadata, Id (OrderItems/10), Order
                        Assert.Equal(4, orderItem.Properties().Count());

                        Assert.Equal("OrderItems", orderItem["@metadata"]["@collection"]);
                        Assert.Equal(110, orderItem["Price"]);
                        Assert.Equal("OrderItems/10", orderItem["Id"]);
                        var nestedOrder = orderItem["Order"];
                        Assert.NotNull(nestedOrder);
                        Assert.Equal(1, nestedOrder.Count());
                        Assert.Equal(440, nestedOrder["Total"]);

                        var orderItem2 = session.Load<JObject>("OrderItems/11");
                        Assert.NotNull(orderItem2);
                    }

                    var collectionStatistics = store.Maintenance.Send(new GetCollectionStatisticsOperation());
                    Assert.Equal(2, collectionStatistics.CountOfDocuments);
                }
            }
        }

        [Theory]
        [InlineData(MigrationProvider.MsSQL)]
        [RequiresNpgSqlInlineData]
        [RequiresMySqlInlineData]
        public async Task CanMigrateLinkOnChild(MigrationProvider provider)
        {
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, "basic"))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(provider, connectionString);
                using (var store = GetDocumentStore())
                {
                    var orderItemCollection = new RootCollection(schemaName, "order_item", "OrderItems")
                    {
                        LinkedCollections = new List<LinkedCollection>
                        {
                            new LinkedCollection(schemaName, "order", RelationType.ManyToOne, new List<string> { "order_id" }, "ParentOrder")
                        }
                    };

                    var orderCollection = new RootCollection(schemaName, "order", "Orders");

                    var db = await GetDocumentDatabaseInstanceFor(store);

                    var settings = new MigrationSettings
                    {
                        Collections = new List<RootCollection>
                        {
                            orderItemCollection,
                            orderCollection
                        }
                    };

                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var schema = driver.FindSchema();
                        ApplyDefaultColumnNamesMapping(schema, settings);
                        await driver.Migrate(settings, schema, db, context);
                    }

                    using (var session = store.OpenSession())
                    {
                        var orderItem = session.Load<JObject>("OrderItems/10");

                        Assert.NotNull(orderItem);
                        // price and metadata, Id (OrderItems/1), Order with link to Orders/1
                        Assert.Equal(4, orderItem.Properties().Count());

                        Assert.Equal("OrderItems", orderItem["@metadata"]["@collection"]);
                        Assert.Equal(110, orderItem["Price"]);
                        Assert.Equal("Orders/1", orderItem["ParentOrder"]);
                        Assert.Equal("OrderItems/10", orderItem["Id"]);


                        var orderItem2 = session.Load<JObject>("OrderItems/11");

                        Assert.NotNull(orderItem2);
                        // price and metadata, Id (OrderItems/2), Order with link to Orders/1
                        Assert.Equal(4, orderItem2.Properties().Count());

                        Assert.Equal("OrderItems", orderItem2["@metadata"]["@collection"]);
                        Assert.Equal(330, orderItem2["Price"]);
                        Assert.Equal("Orders/1", orderItem2["ParentOrder"]);
                        Assert.Equal("OrderItems/11", orderItem2["Id"]);
                    }

                    var collectionStatistics = store.Maintenance.Send(new GetCollectionStatisticsOperation());
                    Assert.Equal(3, collectionStatistics.CountOfDocuments);
                }
            }
        }

        [Theory]
        [InlineData(MigrationProvider.MsSQL)]
        [RequiresNpgSqlInlineData]
        [RequiresOracleSqlInlineData]
        [RequiresMySqlInlineData]
        public async Task NestedEmbedding(MigrationProvider provider)
        {
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, "basic"))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(provider, connectionString);
                using (var store = GetDocumentStore())
                {
                    var collection = new RootCollection(schemaName, "order", "Orders")
                    {
                        NestedCollections = new List<EmbeddedCollection>
                        {
                            new EmbeddedCollection(schemaName, "order_item", RelationType.OneToMany, new List<string> { "order_id" }, "Items")
                            {
                                NestedCollections = new List<EmbeddedCollection>
                                {
                                    new EmbeddedCollection(schemaName, "product", RelationType.ManyToOne, new List<string> { "product_id" }, "Product")
                                }
                            }
                        }
                    };

                    var db = await GetDocumentDatabaseInstanceFor(store);

                    var settings = new MigrationSettings
                    {
                        Collections = new List<RootCollection>
                        {
                            collection
                        }
                    };

                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var schema = driver.FindSchema();
                        ApplyDefaultColumnNamesMapping(schema, settings);
                        await driver.Migrate(settings, schema, db, context);
                    }

                    using (var session = store.OpenSession())
                    {
                        var order = session.Load<JObject>("Orders/1");

                        Assert.NotNull(order);
                        // total and metadata, Id (Orders/1), Items
                        Assert.Equal(4, order.Properties().Count());

                        Assert.Equal("Orders", order["@metadata"]["@collection"]);
                        Assert.Equal(440, order["Total"]);
                        Assert.Equal("Orders/1", order["Id"]);

                        var firstItem = order["Items"][0];
                        Assert.Equal(110, firstItem["Price"]);
                        Assert.Equal(2, firstItem.Count());

                        var firstItemProduct = firstItem["Product"];
                        Assert.NotNull(firstItemProduct);
                        Assert.Equal(1, firstItemProduct.Count());
                        Assert.Equal("Bread", firstItemProduct["Name"]);

                        var secondItem = order["Items"][1];
                        Assert.Equal(330, secondItem["Price"]);
                        Assert.Equal(2, secondItem.Count());

                        var secondItemProduct = secondItem["Product"];
                        Assert.NotNull(secondItemProduct);
                        Assert.Equal(1, secondItemProduct.Count());
                        Assert.Equal("Milk", secondItemProduct["Name"]);
                    }

                    var collectionStatistics = store.Maintenance.Send(new GetCollectionStatisticsOperation());
                    Assert.Equal(1, collectionStatistics.CountOfDocuments);
                }
            }
        }

        [Theory]
        [InlineData(MigrationProvider.MsSQL)]
        [RequiresNpgSqlInlineData]
        [RequiresOracleSqlInlineData]
        [RequiresMySqlInlineData]
        public async Task LinkInsideEmbed(MigrationProvider provider)
        {
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, "basic"))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(provider, connectionString);
                using (var store = GetDocumentStore())
                {
                    var db = await GetDocumentDatabaseInstanceFor(store);

                    var settings = new MigrationSettings
                    {
                        Collections = new List<RootCollection>
                        {
                            new RootCollection(schemaName, "order", "Orders")
                            {
                                NestedCollections = new List<EmbeddedCollection>
                                {
                                    new EmbeddedCollection(schemaName, "order_item", RelationType.OneToMany, new List<string> { "order_id"}, "Items")
                                    {
                                        LinkedCollections = new List<LinkedCollection>
                                        {
                                            new LinkedCollection(schemaName, "product", RelationType.ManyToOne, new List<string> { "product_id" }, "Product")
                                        }
                                    }
                                }
                            },
                            new RootCollection(schemaName, "product", "Products")
                        }
                    };

                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var schema = driver.FindSchema();
                        ApplyDefaultColumnNamesMapping(schema, settings);
                        await driver.Migrate(settings, schema, db, context);
                    }

                    using (var session = store.OpenSession())
                    {
                        var order = session.Load<JObject>("Orders/1");

                        Assert.NotNull(order);
                        // total and metadata, Id (Orders/1), Items
                        Assert.Equal(4, order.Properties().Count());

                        Assert.Equal("Orders", order["@metadata"]["@collection"]);
                        Assert.Equal(440, order["Total"]);
                        Assert.Equal("Orders/1", order["Id"]);

                        var firstItem = order["Items"][0];
                        Assert.Equal(110, firstItem["Price"]);
                        Assert.Equal(2, firstItem.Count());

                        var firstItemProduct = firstItem["Product"];
                        Assert.Equal("Products/100", firstItemProduct);

                        var secondItem = order["Items"][1];
                        Assert.Equal(330, secondItem["Price"]);
                        Assert.Equal(2, secondItem.Count());

                        var secondItemProduct = secondItem["Product"];
                        Assert.Equal("Products/101", secondItemProduct);
                    }

                    var collectionStatistics = store.Maintenance.Send(new GetCollectionStatisticsOperation());
                    Assert.Equal(3, collectionStatistics.CountOfDocuments);
                }
            }
        }
        
        
        [Theory]
        [InlineData(MigrationProvider.MsSQL)]
        [RequiresNpgSqlInlineData]
        [RequiresOracleSqlInlineData]
        [RequiresMySqlInlineData]
        public async Task CanHandleMissingParentEmbed(MigrationProvider provider)
        {
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, "basic"))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(provider, connectionString);
                var query = provider.Equals(MigrationProvider.Oracle) == false ? "update order_item set order_id = null" : "update \"order_item\" set \"order_id\" = null";

                ExecuteSqlQuery(provider, connectionString, query);
                
                using (var store = GetDocumentStore())
                {
                    var collection = new RootCollection(schemaName, "order_item", "OrderItems")
                    {
                        NestedCollections = new List<EmbeddedCollection>
                        {
                            new EmbeddedCollection(schemaName, "order", RelationType.ManyToOne, new List<string> { "order_id" }, "Order")
                        }
                    };

                    var db = await GetDocumentDatabaseInstanceFor(store);

                    var settings = new MigrationSettings
                    {
                        Collections = new List<RootCollection>
                        {
                            collection
                        }
                    };

                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var schema = driver.FindSchema();
                        ApplyDefaultColumnNamesMapping(schema, settings);
                        await driver.Migrate(settings, schema, db, context);
                    }

                    using (var session = store.OpenSession())
                    {
                        var orderItem = session.Load<JObject>("OrderItems/10");

                        Assert.NotNull(orderItem);
                        Assert.True(orderItem.ContainsKey("Order"));
                        Assert.Equal(JTokenType.Null, orderItem["Order"].Type);
                    }
                }
            }
        }

        [Theory]
        [InlineData(MigrationProvider.MsSQL)]
        [RequiresNpgSqlInlineData]
        [RequiresOracleSqlInlineData]
        [RequiresMySqlInlineData]
        public async Task CanHandleMissingParentLink(MigrationProvider provider)
        {
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, "basic"))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(provider, connectionString);
                var query = provider.Equals(MigrationProvider.Oracle) == false ? "update order_item set order_id = null" : "update \"order_item\" set \"order_id\" = null";

                ExecuteSqlQuery(provider, connectionString, query);
                
                
                using (var store = GetDocumentStore())
                {
                    var orderItemCollection = new RootCollection(schemaName, "order_item", "OrderItems")
                    {
                        LinkedCollections = new List<LinkedCollection>
                        {
                            new LinkedCollection(schemaName, "order", RelationType.ManyToOne, new List<string> { "order_id"}, "ParentOrder")
                        }
                    };

                    var orderCollection = new RootCollection(schemaName, "order", "Orders");

                    var db = await GetDocumentDatabaseInstanceFor(store);

                    var settings = new MigrationSettings
                    {
                        Collections = new List<RootCollection>
                        {
                            orderItemCollection,
                            orderCollection
                        }
                    };

                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var schema = driver.FindSchema();
                        ApplyDefaultColumnNamesMapping(schema, settings);
                        await driver.Migrate(settings, schema, db, context);
                    }

                    using (var session = store.OpenSession())
                    {
                        var orderItem = session.Load<JObject>("OrderItems/10");

                        Assert.NotNull(orderItem);
                        Assert.True(orderItem.ContainsKey("ParentOrder"));
                        Assert.Equal(JTokenType.Null, orderItem["ParentOrder"].Type);
                    }
                }
            }
        }
        
        [Theory]
        [InlineData(MigrationProvider.MsSQL)]
        [RequiresNpgSqlInlineData]
        [RequiresOracleSqlInlineData]
        [RequiresMySqlInlineData]
        public async Task CanLimitRows(MigrationProvider provider)
        {
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, "basic"))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(provider, connectionString);
                
                using (var store = GetDocumentStore())
                {
                    var db = await GetDocumentDatabaseInstanceFor(store);

                    var settings = new MigrationSettings
                    {
                        Collections = new List<RootCollection>
                        {
                            new RootCollection(schemaName, "movie", "Movies")
                        },
                        MaxRowsPerTable = 2
                    };

                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var schema = driver.FindSchema();
                        ApplyDefaultColumnNamesMapping(schema, settings);
                        await driver.Migrate(settings, schema, db, context);
                    }

                    using (var session = store.OpenSession())
                    {
                        Assert.Equal(2, session.Advanced.LoadStartingWith<JObject>("Movies/").Length);
                    }
                }
            }
        }
        
        
        [Theory]
        [InlineData(MigrationProvider.MsSQL)]
        [RequiresNpgSqlInlineData]
        [RequiresOracleSqlInlineData]
        [RequiresMySqlInlineData]
        public async Task CanImportWithRelationToNonPrimaryKey(MigrationProvider provider)
        {
            using (WithSqlDatabase(provider, out var connectionString, out string schemaName, "basic"))
            {
                var driver = DatabaseDriverDispatcher.CreateDriver(provider, connectionString);
                
                using (var store = GetDocumentStore())
                {
                    var db = await GetDocumentDatabaseInstanceFor(store);

                    var settings = new MigrationSettings
                    {
                        Collections = new List<RootCollection>
                        {
                            new RootCollection(schemaName, "orders2", "Orders2")
                        }
                    };

                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var schema = driver.FindSchema();
                        var customersSchema = schema.GetTable(schemaName, "customers2");
                        // vat id -> should not be reference! 
                        Assert.Equal(0, customersSchema.References.Count);
                        Assert.True(customersSchema.Columns.Any(x => x.Name == "vatid"));
                        
                        ApplyDefaultColumnNamesMapping(schema, settings);
                        await driver.Migrate(settings, schema, db, context);
                    }

                    using (var session = store.OpenSession())
                    {
                        JObject[] objects = session.Advanced.LoadStartingWith<JObject>("Orders2/");
                        foreach (var jObject in objects)
                        {
                            Assert.True(jObject.GetValue("Customer_vatid").Value<int>() == 55555 || jObject.GetValue("Customer_vatid").Value<int>() == 44444);
                        }
                    }
                }
            }
        }
    }
}
