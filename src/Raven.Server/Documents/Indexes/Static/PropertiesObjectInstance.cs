using System;
using System.Collections.Generic;

using V8.Net;

namespace Raven.Server.Documents.Indexes.Static.JavaScript
{
    public abstract class PropertiesObjectInstance
    {
        private Dictionary<string, InternalHandle> _properties = new Dictionary<string, Handle>();

        public PropertiesObjectInstance()
        {
        }

        ~PropertiesObjectInstance()
        {
            foreach (var kvp in _properties)
                kvp.Value.Dispose();
        }

        public abstract class CustomBinder<T> : ObjectBinder<T>
        where T : class, new()
        {

            public abstract InternalHandle NamedPropertyGetter(ref string propertyName);

            public override V8PropertyAttributes? NamedPropertyQuery(ref string propertyName)
            {
                if (_Handle._properties.Keys().Contains(propertyName))
                    return V8PropertyAttributes.Locked | V8PropertyAttributes.DontEnum;

                return null;
            }

            /*public override InternalHandle NamedPropertyEnumerator()
            {
                //throw new NotSupportedException();
                return Engine.CreateArrayWithDisposal(_Handle._properties.Keys().Select((string propertyName) => Engine.CreateValue(propertyName)));
            }*/

        }
    }
}
