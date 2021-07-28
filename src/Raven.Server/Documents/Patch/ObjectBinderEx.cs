using System;
using System.Collections.Generic;
using V8.Net;

namespace Raven.Server.Documents.Patch
{
    public class ObjectBinderEx<T> : ObjectBinder where T : class
    {
        ObjectBinderEx() : base()
        {
        }

        public T objCLR
        {
            get { return base.Object as T; }
        }

    }

}
