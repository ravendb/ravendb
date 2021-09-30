using System;
using System.Collections.Generic;
using System.Linq;
using V8.Net;
using Raven.Server.Documents.Patch;
using Raven.Server.Extensions;

namespace Raven.Server.Documents.Indexes.Static.JavaScript
{
    public abstract class ObjectInstanceBase: IDisposable
#if DEBUG
    , IV8DebugInfo
#endif
    {
        public abstract class CustomBinder<T> : ObjectBinderEx<T>
        where T : ObjectInstanceBase
        {

            public CustomBinder() : base()
            {
            }

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


        private bool _disposed = false;

        bool IsImplicitNull;
        protected DictionaryDisposeValueIHV8<string> _properties = new DictionaryDisposeValueIHV8<string>();

#if DEBUG
        private V8EntityID _SelfID;
#endif

        public ObjectInstanceBase(bool isImplicitNull = true)
        {
            IsImplicitNull = isImplicitNull;
            GC.SuppressFinalize(_properties);
        }

        public abstract InternalHandle NamedPropertyGetterOnce(V8EngineEx engine, ref string propertyName);

        public virtual InternalHandle NamedPropertyGetter(V8EngineEx engine, ref string propertyName)
        {
            if (!_properties.TryGetValue(propertyName, out InternalHandle jsValue))
            {
                jsValue = NamedPropertyGetterOnce((V8EngineEx)engine, ref propertyName);
                if (!jsValue.IsEmpty)
                    _properties.Add(propertyName, ref jsValue);
            }

            if (jsValue.IsEmpty) {
                return IsImplicitNull ? engine.ImplicitNull.CreateHandle() : jsValue;
            }

            return jsValue;
        }

        ~ObjectInstanceBase()
        {            
            Dispose(false);
        }


        public void Dispose()
        {  
            Dispose(true);
        }

        protected void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            _properties.Dispose();

            if (disposing) {
                _properties = null;

                //GC.SuppressFinalize(this);
            }

            _disposed = true;
        }

#if DEBUG
        public V8EntityID SelfID
        {
            get {
                return _SelfID;
            } 

            set {
                _SelfID = value;
            } 
        }

        public V8EntityID ParentID
        {
            get {return null;} 
        }

        public List<V8EntityID> ChildIDs
        {
            get {
                var res = new List<V8EntityID>();

                var countProps = _properties?.Count ?? 0;
                if (countProps <= 0)
                    return res;

                foreach (var kvp in _properties) {
                    InternalHandle h = kvp.Value;
                    res.Add(new V8EntityID(h.HandleID, h.ObjectID));

                    if (!(h.IsDisposed || h.IsCLRDisposed) && h.IsArray) {
                        for (int j = 0; j < h.ArrayLength; j++)
                        {
                            using (var jsItem = h.GetProperty(j))
                            {
                                res.Add(new V8EntityID(jsItem.HandleID, jsItem.ObjectID));
                            }
                        }
                    }
                }
                //Engine.ForceV8GarbageCollection();
                return res;
            } 
        }

        public string Summary
        {
            get {
                string desc = "";
                return desc;
            }
        }
#endif

    }
}
