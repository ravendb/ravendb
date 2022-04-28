using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;

namespace Raven.Server.Web.Studio.Processors;

internal class StudioStudioDatabaseTasksHandlerProcessorForGetFolderPathOptionsForDatabaseAdmin : AbstractHandlerProcessor<RequestHandler>
{
    public StudioStudioDatabaseTasksHandlerProcessorForGetFolderPathOptionsForDatabaseAdmin([NotNull] RequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        var type = RequestHandler.GetStringValuesQueryString("type", required: false);
        var isBackupFolder = RequestHandler.GetBoolValueQueryString("backupFolder", required: false) ?? false;
        var path = RequestHandler.GetStringQueryString("path", required: false);

        await StudioTasksHandler.GetFolderPathOptionsInternal(RequestHandler.ServerStore, type, isBackupFolder, path, RequestHandler.RequestBodyStream, RequestHandler.ResponseBodyStream);
    }
}
