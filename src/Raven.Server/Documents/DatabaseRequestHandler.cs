using System;
using System.Threading.Tasks;
using Raven.Server.Exceptions;
using Raven.Server.ServerWide;
using Raven.Server.Web;

namespace Raven.Server.Documents
{
    public abstract class DatabaseRequestHandler : RequestHandler
    {
        protected ContextPool ContextPool;
        protected DocumentsStorage DocumentsStorage;

        public override void Init(RequestHandlerContext context)
        {
            base.Init(context);

            DocumentsStorage = context.DocumentsStorage;
            ContextPool = DocumentsStorage?.ContextPool;
        }

    }
}