using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using NJsonSchema;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.SchemaValidation;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.SchemaValidation;

public class SchemaValidationBasicTests : RavenTestBase
{
    public SchemaValidationBasicTests(ITestOutputHelper output) : base(output) { }

    [RavenTheory(RavenTestCategory.ClientApi, LicenseRequired = true)]
    [InlineData("Users")]
    [InlineData("users")]
    public async Task Store(string collection)
    {
        var schema = JsonSchema.FromType<User>();
        var schemaData = schema.ToJson();

        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(new SchemaValidationConfiguration
            {
                ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>()
                {
                    {collection, new SchemaDefinition
                    {
                        Schema = schemaData
                    }}
                }
            }));


            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Age = 17 }, "users/1");
                var error = await Assert.ThrowsAsync<SchemaValidationException>(async () => await session.SaveChangesAsync());
                Assert.Contains("The value '17' at 'Age' should be greater than or equal to 21.0.", error.Message);
            }

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Age = 80 }, "users/1");
                var error = await Assert.ThrowsAsync<SchemaValidationException>(async () => await session.SaveChangesAsync());
                Assert.Contains("The value '80' at 'Age' should be less than or equal to 67.0.", error.Message);
            }

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Age = 39 }, "users/1");
                await session.SaveChangesAsync();
            }
        }
    }

    [RavenTheory(RavenTestCategory.ClientApi, LicenseRequired = true)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task StoreClusterTransaction(Options options)
    {
        var schema = JsonSchema.FromType<User>();
        var schemaData = schema.ToJson();

        using (var store = GetDocumentStore(options))
        {
            await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(new SchemaValidationConfiguration
            {
                ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>()
                {
                    {"Users", new SchemaDefinition
                    {
                        Schema = schemaData
                    }}
                }
            }));

            using (var session = store.OpenAsyncSession(new SessionOptions{ TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new User { Age = 17 }, "users/1");
                var error = await Assert.ThrowsAsync<SchemaValidationException>(async () => await session.SaveChangesAsync());
                Assert.Contains("The value '17' at 'Age' should be greater than or equal to 21.0.", error.Message);
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new User { Age = 80 }, "users/1");
                var error = await Assert.ThrowsAsync<SchemaValidationException>(async () => await session.SaveChangesAsync());
                Assert.Contains("The value '80' at 'Age' should be less than or equal to 67.0.", error.Message);
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new User { Age = 39 }, "users/1");
                await session.SaveChangesAsync();
            }

            var configuration = await store.Maintenance.SendAsync(new GetSchemaValidationConfiguration());
            configuration.ValidatorsPerCollection["Companies"] = new SchemaDefinition
            {
                Schema = schemaData
            };
            configuration.ValidatorsPerCollection["Users"].Disabled = true;
            await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration));

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new User { Age = 20 }, "users/2");
                await session.SaveChangesAsync();
            }
        }
    }

    [RavenFact(RavenTestCategory.ClientApi, LicenseRequired = true)]
    public async Task DisableSchemaAfterCreation()
    {
        var schema = JsonSchema.FromType<User>();
        var schemaData = schema.ToJson();

        using (var store = GetDocumentStore())
        {
            var configuration = new SchemaValidationConfiguration
            {
                ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>
                {
                    {
                        "Users", new SchemaDefinition
                        {
                            Schema = schemaData
                        }
                    },
                    {
                        "Orders", new SchemaDefinition
                        {
                            Schema = schemaData
                        }
                    }
                }
            };
            await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration));

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Age = 17 }, "users/1");
                var error = await Assert.ThrowsAsync<SchemaValidationException>(async () => await session.SaveChangesAsync());
                Assert.Contains("The value '17' at 'Age' should be greater than or equal to 21.0.", error.Message);
            }

            configuration.ValidatorsPerCollection["Users"].Disabled = true;
            await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration));

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Age = 80 }, "users/1");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Age = 39 }, "users/1");
                await session.SaveChangesAsync();
            }
        }
    }

    [RavenFact(RavenTestCategory.ClientApi, LicenseRequired = true)]
    public async Task CanStartWithDisabledSchema()
    {
        var schema = JsonSchema.FromType<User>();
        var schemaData = schema.ToJson();

        using (var store = GetDocumentStore())
        {
            var configuration = new SchemaValidationConfiguration
            {
                ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>
                {
                    {
                        "Users", new SchemaDefinition
                        {
                            Disabled = true,
                            Schema = schemaData
                        }
                    },
                    {
                        "Orders", new SchemaDefinition
                        {
                            Schema = schemaData
                        }
                    }
                }
            };
            await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration));

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Age = 17 }, "users/1");
                await session.SaveChangesAsync();
            }

            configuration.ValidatorsPerCollection["Users"].Disabled = false;
            await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(configuration));

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Age = 80 }, "users/1");
                var error = await Assert.ThrowsAsync<SchemaValidationException>(async () => await session.SaveChangesAsync());
                Assert.Contains("The value '80' at 'Age' should be less than or equal to 67.0.", error.Message);
            }
        }
    }

    [RavenFact(RavenTestCategory.Configuration, LicenseRequired = true)]
    public async Task LastModifiedTimeDoesNotChangeOnUnrelatedDatabaseRecordUpdate()
    {
        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(new SchemaValidationConfiguration
            {
                ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>
                {
                    { "Users", new SchemaDefinition { Schema = JsonSchema.FromType<User>().ToJson() } }
                }
            }));

            DateTime lastModifiedTime = await GetSchemaLastModifiedTimeAsync(store, "Users");
            Assert.NotEqual(default, lastModifiedTime);

            // putting an index updates the database record without touching the schema validation configuration
            await store.Maintenance.SendAsync(new PutIndexesOperation(new IndexDefinition
            {
                Name = "Users/ByAge",
                Maps = { "from u in docs.Users select new { u.Age }" }
            }));

            Assert.Equal(lastModifiedTime, await GetSchemaLastModifiedTimeAsync(store, "Users"));
        }
    }

    [RavenFact(RavenTestCategory.Configuration, LicenseRequired = true)]
    public async Task LastModifiedTimeChangesOnlyForModifiedDefinitions()
    {
        string schema = JsonSchema.FromType<User>().ToJson();

        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(new SchemaValidationConfiguration
            {
                ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>
                {
                    { "Users", new SchemaDefinition { Schema = schema } },
                    { "Companies", new SchemaDefinition { Schema = schema } }
                }
            }));

            DateTime usersLastModifiedTime = await GetSchemaLastModifiedTimeAsync(store, "Users");
            DateTime companiesLastModifiedTime = await GetSchemaLastModifiedTimeAsync(store, "Companies");

            await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(new SchemaValidationConfiguration
            {
                ValidatorsPerCollection = new Dictionary<string, SchemaDefinition>
                {
                    { "Users", new SchemaDefinition { Schema = schema } },
                    { "Companies", new SchemaDefinition { Schema = schema, Disabled = true } }
                }
            }));

            Assert.Equal(usersLastModifiedTime, await GetSchemaLastModifiedTimeAsync(store, "Users"));

            DateTime companiesLastModifiedTimeAfterChange = await GetSchemaLastModifiedTimeAsync(store, "Companies");
            Assert.True(companiesLastModifiedTimeAfterChange > companiesLastModifiedTime,
                $"Expected the last modified time of the changed definition to move forward, but it went from {companiesLastModifiedTime:O} to {companiesLastModifiedTimeAfterChange:O}.");
        }
    }

    private static async Task<DateTime> GetSchemaLastModifiedTimeAsync(IDocumentStore store, string collection)
    {
        DatabaseRecordWithEtag record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        return record.SchemaValidation.ValidatorsPerCollection[collection].LastModifiedTime;
    }

    private class User
    {
        [Range(21, 67)]
        public int Age { get; set; }
    }
}
