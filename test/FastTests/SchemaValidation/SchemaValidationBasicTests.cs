using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using NJsonSchema;
using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Client.Exceptions.SchemaValidation;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.SchemaValidation;

public class SchemaValidationBasicTests : RavenTestBase
{
    public SchemaValidationBasicTests(ITestOutputHelper output) : base(output) { }

    [RavenTheory(RavenTestCategory.ClientApi)]
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

    private class User
    {
        [Range(21, 67)]
        public int Age { get; set; }
    }
}
