using FastTests;
using Newtonsoft.Json;
using Raven.Quill.AiHelper.Migration.Planning;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class MigrationPlanTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public void Loading_a_plan_with_conventions_does_not_hand_them_to_the_next_plan()
    {
        // The document store deserializes with ObjectCreationHandling.Auto, which fills a property's
        // existing value in place rather than replacing it.
        var serializer = JsonSerializer.Create(new JsonSerializerSettings { ObjectCreationHandling = ObjectCreationHandling.Auto });
        const string stored = """{"ConversationId":"c/1","Conventions":{"PropertyCase":"SnakeCase","PropertyLanguage":"Spanish"}}""";

        var loaded = serializer.Deserialize<MigrationPlan>(new JsonTextReader(new StringReader(stored)));
        var fresh = new MigrationPlan();

        Assert.Equal(PropertyCase.SnakeCase, loaded!.Conventions.PropertyCase);
        Assert.Equal(PropertyCase.Unspecified, fresh.Conventions.PropertyCase);
        Assert.Null(fresh.Conventions.PropertyLanguage);
    }
}
