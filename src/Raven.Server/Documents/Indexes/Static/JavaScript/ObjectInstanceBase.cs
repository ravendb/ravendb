using System;
using System.Collections.Generic;
using System.Linq;
using V8.Net;
using Raven.Server.Documents.Patch;
using Raven.Server.Extensions;

namespace Raven.Server.Documents.Indexes.Static.JavaScript
{
    public abstract class ObjectInstanceBase
    {
        protected DictionaryDisposeValueIHV8<string> _properties = new DictionaryDisposeValueIHV8<string>();

        public ObjectInstanceBase()
        {
        }

        public abstract InternalHandle NamedPropertyGetter(V8EngineEx engine, ref string propertyName);

        public abstract class CustomBinder<T> : ObjectBinderEx<T>
        where T : ObjectInstanceBase
        {

            public override InternalHandle NamedPropertyGetter(ref string propertyName)
            {
                return ObjCLR.NamedPropertyGetter((V8EngineEx)Engine, ref propertyName);
            }

            public override V8PropertyAttributes? NamedPropertyQuery(ref string propertyName)
            {
                if (ObjCLR._properties.ContainsKey(propertyName))
                    return V8PropertyAttributes.Locked | V8PropertyAttributes.DontEnum;

                return null;
            }

            public override InternalHandle NamedPropertyEnumerator()
            {
                //throw new NotSupportedException();
                return ((V8EngineEx)Engine).CreateArrayWithDisposal(ObjCLR._properties.Keys.Select((string propertyName) => Engine.CreateValue(propertyName)).ToArray());
            }

        }
    }
}
