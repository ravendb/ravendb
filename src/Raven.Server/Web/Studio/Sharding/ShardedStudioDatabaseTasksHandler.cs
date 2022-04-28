using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Studio.Processors;
using Raven.Server.Web.Studio.Sharding.Processors;

namespace Raven.Server.Web.Studio.Sharding;

public class ShardedStudioDatabaseTasksHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/admin/studio-tasks/folder-path-options", "POST")]
    public async Task GetFolderPathOptionsForDatabaseAdmin()
    {
        using (var processor = new StudioStudioDatabaseTasksHandlerProcessorForGetFolderPathOptionsForDatabaseAdmin(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/studio-tasks/indexes/configuration/defaults", "GET")]
    public async Task GetIndexDefaults()
    {
        using (var processor = new ShardedStudioDatabaseTasksHandlerProcessorForGetIndexDefaults(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/studio-tasks/suggest-conflict-resolution", "GET")]
    public async Task SuggestConflictResolution()
    {
        using (var processor = new ShardedStudioDatabaseTasksHandlerProcessorForGetSuggestConflictResolution(this))
            await processor.ExecuteAsync();
    }
}
