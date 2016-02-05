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

        protected long? GetLongFromHeaders(string name)
        {
            long? etag = null;
            var etags = HttpContext.Request.Headers[name];
            if (etags.Count != 0)
            {
                long result;
                if (long.TryParse(etags[0], out result) == false)
                    throw new ArgumentException(
                        "Could not parse header '" + name + "' header as int64, value was: " + etags[0]);
                etag = result;
            }
            return etag;
        }

        protected int GetStart(int defaultValue = 0)
        {
            return GetIntQueryString("start", defaultValue);
        }

        protected int GetPageSize(int defaultValue = 25)
        {
            return GetIntQueryString("pageSize", defaultValue);
        }

        protected int GetIntQueryString(string name, int defaultValue)
        {
            var val = HttpContext.Request.Query[name];
            if (val.Count != 0)
            {
                int result;
                if (int.TryParse(val[0], out result) == false)
                    throw new ArgumentException(
                        string.Format("Could not parse query string '{0}' header as int32, value was: {1}", name, val[0]));
                return result;
            }
            return defaultValue;
        }


        protected long GetLongQueryString(string name)
        {
            var val = HttpContext.Request.Query[name];
            if (val.Count != 0)
            {
                int result;
                if (int.TryParse(val[0], out result) == false)
                    throw new ArgumentException(
                        string.Format("Could not parse query string '{0}' header as int32, value was: {1}", name, val[0]));
                return result;
            }
            throw new ArgumentException($"Query string {name} is mandatory, but wasn't specified");
        }
    }
}