using System;
using System.Collections.Generic;
using V8.Net;

namespace Raven.Server.Documents.Patch.V8
{
    public class ObjectBinderEx<T> : ObjectBinder where T : class
    {
        public ObjectBinderEx() : base()
        {
        }
        public T ObjClr
        {
            get { return base.Object as T; }
        }

    }

}
