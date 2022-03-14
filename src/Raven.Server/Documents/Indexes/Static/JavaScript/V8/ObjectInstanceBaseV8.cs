using System;
using System.Collections.Generic;
using System.Linq;
using V8.Net;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.Extensions.V8;

namespace Raven.Server.Documents.Indexes.Static.JavaScript.V8
{
    public abstract class ObjectInstanceBaseV8: IObjectInstance // TODO [shlomo] refactor to make it ObjectBinder itself
    , IV8DebugInfo
    {
        public abstract class CustomBinder<T> : ObjectBinderEx<T>
        where T : ObjectInstanceBaseV8
        {

            public CustomBinder() : base()
            {
            }

            public override InternalHandle NamedPropertyGetter(ref string propertyName)
            {
                try
                {
                    return ObjClr.NamedPropertyGetter((V8EngineEx)Engine, ref propertyName);
                }
                catch (Exception e)
                {
                    var engineEx = (V8EngineEx)Engine;
                    engineEx.Context.JsContext.LastException = e;
                    return Engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            public override V8PropertyAttributes? NamedPropertyQuery(ref string propertyName)
            {
                try
                {
                    if (ObjClr._properties.ContainsKey(propertyName))
                        return V8PropertyAttributes.Locked | V8PropertyAttributes.DontEnum;

                    return null;
                }
                catch (Exception e)
                {
                    var engineEx = (V8EngineEx)Engine;
                    engineEx.Context.JsContext.LastException = e;
                    return null;
                }
            }

            public override InternalHandle NamedPropertyEnumerator()
            {
                try
                {
                    return ((V8EngineEx)Engine).CreateArrayWithDisposal(ObjClr._properties.Keys.Select((string propertyName) => Engine.CreateValue(propertyName)).ToArray());
                }
                catch (Exception e)
                {
                    var engineEx = (V8EngineEx)Engine;
                    engineEx.Context.JsContext.LastException = e;
                    return Engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

        }

        protected JavaScriptUtilsV8 _javaScriptUtils;
        protected V8Engine _engine;
        protected V8EngineEx EngineEx;
        private bool _disposed = false;

        bool IsImplicitNull;
        protected DictionaryCloningValueIHV8<string> _properties = new DictionaryCloningValueIHV8<string>();

        private V8EntityID _SelfID; // for memory checks

        public ObjectInstanceBaseV8(V8EngineEx engineEx, bool isImplicitNull = true)
        {
            EngineEx = engineEx;
            _engine = EngineEx;
                
            IsImplicitNull = isImplicitNull;
            GC.SuppressFinalize(_properties);
        }

        public IJsEngineHandle EngineHandle => EngineEx;

        public JsHandle CreateJsHandle(bool keepAlive = false)
        {
            return new JsHandle(CreateObjectBinder(keepAlive));
        }

        public abstract InternalHandle CreateObjectBinder(bool keepAlive = false);

        public abstract InternalHandle NamedPropertyGetterOnce(V8EngineEx engine, ref string propertyName);

        public InternalHandle NamedPropertyGetter(V8EngineEx engineEx, ref string propertyName)
        {
            
            if (!_properties.TryGetValue(propertyName, out InternalHandle jsValue))
            {
                jsValue = NamedPropertyGetterOnce(EngineEx, ref propertyName);
                if (!jsValue.IsEmpty)
                {
                    using (jsValue)
                        _properties.Add(propertyName, jsValue);
                }
            }

            if (jsValue.IsEmpty)
                return IsImplicitNull ? engineEx.Context.ImplicitNullV8().Clone() : jsValue;

            return jsValue.Clone();
        }

        ~ObjectInstanceBaseV8()
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

            if (disposing)
            {
                _properties = null;
                
                GC.SuppressFinalize(this);
            }

            _disposed = true;
        }

        public V8EntityID SelfID
        {
            get{ return _SelfID; }
            set { _SelfID = value; } 
        }

        public V8EntityID ParentID
        {
            get { return null; } 
        }

        public List<V8EntityID> ChildIDs
        {
            get
            {
                var res = new List<V8EntityID>();

                var countProps = _properties?.Count ?? 0;
                if (countProps <= 0)
                    return res;

                foreach (var kvp in _properties)
                {
                    InternalHandle h = kvp.Value;
                    res.Add(new V8EntityID(h.HandleID, h.ObjectID));

                    if (!(h.IsDisposed || h.IsCLRDisposed) && h.IsArray)
                    {
                        for (int j = 0; j < h.ArrayLength; j++)
                        {
                            using (var jsItem = h.GetProperty(j))
                            {
                                res.Add(new V8EntityID(jsItem.HandleID, jsItem.ObjectID));
                            }
                        }
                    }
                }
                return res;
            } 
        }

        public string Summary
        {
            get
            {
                string desc = "";
                return desc;
            }
        }
    }
}
