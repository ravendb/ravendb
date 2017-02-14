using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Logging;

namespace Raven.Server.Files.Handlers
{
    public abstract class FileSystemRequestHandler : RequestHandler
    {
        protected FilesContextPool ContextPool;
        protected FileSystem FileSystem;
        protected Logger Logger;

        public override void Init(RequestHandlerContext context)
        {
            base.Init(context);

            FileSystem = context.FileSystem;
            ContextPool = FileSystem.FilesStorage.ContextPool;
            Logger = LoggingSource.Instance.GetLogger(FileSystem.Name, GetType().FullName);
        }
    }
}