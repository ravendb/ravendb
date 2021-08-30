using System;
using System.Collections.Generic;
using V8.Net;

namespace Raven.Server.Documents.Patch
{
    public class ObjectBinderEx<T> : ObjectBinder where T : class
    {
        public ObjectBinderEx() : base()
        {
        }

        public T ObjCLR
        {
            get { return base.Object as T; }
        }

        public override void OnDispose()
        {
            if (base.Object is IDisposable r) {
                r.Dispose();
            }

            base.OnDispose();
        }

    }

}
