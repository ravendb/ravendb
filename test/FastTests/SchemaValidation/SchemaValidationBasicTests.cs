using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using NJsonSchema;
using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Client.Exceptions;
using Raven.Server.SchemaValidation;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using SVC = Raven.Server.SchemaValidation.SchemaValidatorConstants;
namespace FastTests.SchemaValidation;

public class SchemaValidationBasicTests : RavenTestBase
{
    public SchemaValidationBasicTests(ITestOutputHelper output) : base(output) { }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task Crud()
    {
        var schema = JsonSchema.FromType<User>();
        var schemaData = schema.ToJson();

        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new ConfigureSchemaValidationOperation(new SchemaValidationConfiguration
            {
                ValidatorsByCollection = new Dictionary<string, SchemaValidationConfiguration.Validator>()
                {
                    {"Users", new SchemaValidationConfiguration.Validator
                    {
                        SchemaDefinition = schemaData
                    }}
                }
            }));


            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Age = 17 }, "users/1");
                await Assert.ThrowsAsync<RavenException>(async () => await session.SaveChangesAsync());
            }
        }
    }

    private class User
    {
        [Required]
        public string Name { get; set; }

        [Range(18, int.MaxValue)]
        public int Age { get; set; }
    }
}
