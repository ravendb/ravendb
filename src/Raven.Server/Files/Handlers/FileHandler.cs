using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Voron.Exceptions;

namespace Raven.Server.Files.Handlers
{
    public class FileHandler : FileSystemRequestHandler
    {
        [RavenAction("/fs/*/files", "DELETE", "/fs/{fileSystemName:string}/files?name={fileName:string}")]
        public async Task Delete()
        {
            FilesOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

                var etag = GetLongFromHeaders("If-Match");

                var cmd = new MergedDeleteCommand
                {
                    Name = name,
                    FileSystem = FileSystem,
                    ExpectedEtag = etag
                };

                await FileSystem.TxMerger.Enqueue(cmd);

                cmd.ExceptionDispatchInfo?.Throw();

                NoContentStatus();
            }
        }

        private class MergedDeleteCommand : FilesTransactionsMerger.MergedTransactionCommand
        {
            public string Name;
            public long? ExpectedEtag;
            public FileSystem FileSystem;
            public ExceptionDispatchInfo ExceptionDispatchInfo;

            public override void Execute(FilesOperationContext context)
            {
                try
                {
                    FileSystem.FilesStorage.Delete(context, Name, ExpectedEtag);
                }
                catch (ConcurrencyException e)
                {
                    ExceptionDispatchInfo = ExceptionDispatchInfo.Capture(e);
                }
            }
        }

    }
}