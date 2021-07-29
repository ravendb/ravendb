using System;
using System.Collections.Generic;
using V8.Net;
using Raven.Server.Documents.Patch;

namespace Raven.Server.Documents.Indexes.Static.JavaScript
{
    public abstract class ObjectInstanceBase
    {
        protected DictionaryDisposeValueIHV8<string> _properties = new DictionaryDisposeValueIHV8<string>();

        JavaScriptUtils JavaScriptUtils;

        public ObjectInstanceBase(JavaScriptUtils javaScriptUtils = null)
        {
            JavaScriptUtils = javaScriptUtils;
        }

        public abstract class CustomBinder<T> : ObjectBinderEx<T>
        where T : class
        {

            public abstract InternalHandle NamedPropertyGetter(ref string propertyName);

            public override V8PropertyAttributes? NamedPropertyQuery(ref string propertyName)
            {
                if (objCLR._properties.Keys().Contains(propertyName))
                    return V8PropertyAttributes.Locked | V8PropertyAttributes.DontEnum;

                return null;
            }

            /*public override InternalHandle NamedPropertyEnumerator()
            {
                //throw new NotSupportedException();
                return Engine.CreateArrayWithDisposal(objCLR._properties.Keys().Select((string propertyName) => Engine.CreateValue(propertyName)));
            }*/

        }
    }
}
