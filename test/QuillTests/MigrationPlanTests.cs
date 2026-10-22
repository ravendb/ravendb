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

    private static MigrationPlan PlanWithOrders() =>
        MigrationSamples.Plan("c/1", new PlanEntry { Collection = "Orders", Version = 1, Config = MigrationSamples.ValidOrders() });

    private static Raven.Client.Documents.Operations.CdcSink.CdcSinkTableConfig Named(string collection)
    {
        var config = MigrationSamples.ValidOrders();
        config.CollectionName = collection;
        return config;
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_collection_added_and_removed_before_the_next_turn_tells_the_planner_nothing()
    {
        var plan = PlanWithOrders();

        plan.ApplyUserEdit("Customers", Named("Customers"));
        plan.ApplyUserEdit("Customers", config: null);

        Assert.Empty(plan.PendingUserEdits);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_renamed_collection_that_is_then_removed_is_removed_under_the_name_the_planner_knows()
    {
        var plan = PlanWithOrders();

        plan.ApplyUserEdit("Orders", Named("Sales"));
        plan.ApplyUserEdit("Sales", config: null);

        Assert.Equal([new UserEdit("Orders", UserEditKind.Removed)], plan.PendingUserEdits);
        Assert.Empty(plan.CurrentEntries());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_removed_collection_added_back_is_an_edit()
    {
        var plan = PlanWithOrders();

        plan.ApplyUserEdit("Orders", config: null);
        plan.ApplyUserEdit("Orders", Named("Orders"));

        Assert.Equal([new UserEdit("Orders", UserEditKind.Edited)], plan.PendingUserEdits);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Renaming_twice_keeps_the_original_name_and_renaming_back_is_an_edit()
    {
        var plan = PlanWithOrders();

        plan.ApplyUserEdit("Orders", Named("Sales"));
        plan.ApplyUserEdit("Sales", Named("Purchases"));
        Assert.Equal([new UserEdit("Purchases", UserEditKind.Renamed, "Orders")], plan.PendingUserEdits);

        plan.ApplyUserEdit("Purchases", Named("Orders"));
        Assert.Equal([new UserEdit("Orders", UserEditKind.Edited)], plan.PendingUserEdits);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void An_edit_after_an_addition_stays_an_addition()
    {
        var plan = PlanWithOrders();

        plan.ApplyUserEdit("Customers", Named("Customers"));
        plan.ApplyUserEdit("Customers", Named("Customers"));

        Assert.Equal([new UserEdit("Customers", UserEditKind.Added)], plan.PendingUserEdits);
        Assert.Equal(2, plan.CurrentEntries().Single(e => e.Collection == "Customers").Version);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Renaming_onto_a_name_the_user_removed_keeps_both_and_a_later_removal_removes_the_renamed_one()
    {
        var plan = MigrationSamples.Plan("c/1",
            new PlanEntry { Collection = "Customers", Version = 1, Config = Named("Customers") },
            new PlanEntry { Collection = "Clients", Version = 1, Config = Named("Clients") });

        plan.ApplyUserEdit("Customers", config: null);
        plan.ApplyUserEdit("Clients", Named("Customers"));
        Assert.Equal([new UserEdit("Customers", UserEditKind.Removed), new UserEdit("Customers", UserEditKind.Renamed, "Clients")], plan.PendingUserEdits);

        plan.ApplyUserEdit("Customers", config: null);
        Assert.Equal([new UserEdit("Customers", UserEditKind.Removed), new UserEdit("Clients", UserEditKind.Removed)], plan.PendingUserEdits);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Changing_only_the_case_of_a_name_renames_it()
    {
        var plan = PlanWithOrders();

        Assert.Null(plan.ApplyUserEdit("Orders", Named("orders")));

        Assert.Equal("orders", Assert.Single(plan.CurrentEntries()).Collection);
        Assert.Equal([new UserEdit("orders", UserEditKind.Renamed, "Orders")], plan.PendingUserEdits);
    }
}
