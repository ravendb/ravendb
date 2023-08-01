using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.PeriodicBackup;
using Sparrow.Json;

namespace Raven.Server.Web.Studio.Processors;

internal sealed class StudioDatabaseTasksHandlerProcessorForGetFolderPathOptionsForOperator : StudioDatabaseTasksHandlerProcessorForGetFolderPathOptionsForDatabaseAdmin
{
    public StudioDatabaseTasksHandlerProcessorForGetFolderPathOptionsForOperator([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override ValueTask<RavenCommand<FolderPathOptions>> CreateCommandForNodeAsync(string nodeTag, JsonOperationContext context)
    {
        return new ValueTask<RavenCommand<FolderPathOptions>>(new GetFolderPathOptionsForOperatorCommand(GetPeriodicBackupConnectionType(), GetPath(), IsBackupFolder(), nodeTag));
    }

    private class GetFolderPathOptionsForOperatorCommand : AbstractGetFolderPathOptionsCommand
    {
        public GetFolderPathOptionsForOperatorCommand(PeriodicBackupConnectionType connectionType, string path, bool isBackupFolder, string nodeTag)
            : base(connectionType, path, isBackupFolder, nodeTag)
        {
        }

        protected override string GetBaseUrl(ServerNode node) => $"{node.Url}/admin/studio-tasks/folder-path-options";
    }
}
