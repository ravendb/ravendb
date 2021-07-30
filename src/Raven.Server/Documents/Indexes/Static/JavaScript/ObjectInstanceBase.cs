using System;
using System.Collections.Generic;
using V8.Net;
using Raven.Server.Documents.Patch;

namespace Raven.Server.Documents.Indexes.Static.JavaScript
{
    public abstract class ObjectInstanceBase
    {
        protected DictionaryDisposeValueIHV8<string> _properties = new DictionaryDisposeValueIHV8<string>();

        public ObjectInstanceBase()
        {
        }

        public abstract class CustomBinder<T> : ObjectBinderEx<T>
        where T : class
        {

            public virtual InternalHandle NamedPropertyGetter(ref string propertyName)
            {
                return ObjCLR.NamedPropertyGetter(Engine, ref propertyName);
            }

            public override V8PropertyAttributes? NamedPropertyQuery(ref string propertyName)
            {
                if (ObjCLR._properties.Keys().Contains(propertyName))
                    return V8PropertyAttributes.Locked | V8PropertyAttributes.DontEnum;

                return null;
            }

            public override InternalHandle NamedPropertyEnumerator()
            {
                //throw new NotSupportedException();
                return Engine.CreateArrayWithDisposal(ObjCLR._properties.Keys().Select((string propertyName) => Engine.CreateValue(propertyName)));
            }

        }
    }
}
