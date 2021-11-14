using System;
using System.Collections.Generic;
using V8.Net;

namespace Raven.Server.Documents.Indexes.Static.JavaScript.V8
{
    public sealed class DynamicJsNullV8 : V8ManagedObject, IEquatable<InternalHandle>, IEquatable<DynamicJsNullV8>
    {
        public bool _isExplicitNull;

        public void SetKind(bool isExplicitNull)
        {
            _isExplicitNull = isExplicitNull;
        }

        public bool IsExplicitNull { get { return _isExplicitNull; } }
        
        public override string ToString()
        {
            return "null";
        }

        public InternalHandle CreateHandle()
        {
            return InternalHandle.Clone();
        }
        
        public IDictionary<string, IJSProperty> NamedProperties
        {
            get
            {
                throw new NotSupportedException();
            }
            protected set
            {
                throw new NotSupportedException();
            }
        }

        public override InternalHandle NamedPropertyGetter(ref string propertyName)
        {
            return CreateHandle();
        }
        
        public override InternalHandle NamedPropertySetter(ref string propertyName, InternalHandle value, V8PropertyAttributes attributes = V8PropertyAttributes.Undefined)
        {
            throw new NotSupportedException();
        }

        public override V8PropertyAttributes? NamedPropertyQuery(ref string propertyName)
        {
            throw new NotSupportedException();
        }

        public override bool? NamedPropertyDeleter(ref string propertyName)
        {
            throw new NotSupportedException();
        }

        public override InternalHandle NamedPropertyEnumerator()
        {
            throw new NotSupportedException();
        }

        // --------------------------------------------------------------------------------------------------------------------

        public override InternalHandle IndexedPropertyGetter(int index)
        {
            return CreateHandle();
        }
        
        public override InternalHandle IndexedPropertySetter(int index, InternalHandle value, V8PropertyAttributes attributes = V8PropertyAttributes.Undefined)
        {
            throw new NotSupportedException();
        }

        public override V8PropertyAttributes? IndexedPropertyQuery(int index)
        {
            throw new NotSupportedException();
        }

        public override bool? IndexedPropertyDeleter(int index)
        {
            throw new NotSupportedException();
        }

        public override InternalHandle IndexedPropertyEnumerator()
        {
            throw new NotSupportedException();
        }
        
        // --------------------------------------------------------------------------------------------------------------------
        public new InternalHandle Call(string functionName, InternalHandle _this, params InternalHandle[] args)
        {
            return CreateHandle();
        }

        public new InternalHandle StaticCall(string functionName, params InternalHandle[] args)
        {
            return CreateHandle();
        }

        public new InternalHandle Call(InternalHandle _this, params InternalHandle[] args)
        {
            return CreateHandle();
        }

        public new InternalHandle StaticCall(params InternalHandle[] args)
        {
            return CreateHandle();
        }

        /*public FunctionTemplate FunctionTemplate { get { return (FunctionTemplate)base.Template; } }

        public JSFunction Callback { get; set; }
        
        public override InternalHandle StaticCall(params InternalHandle[] args) { return CreateHandle(); }

        public override InternalHandle Call(InternalHandle _this, params InternalHandle[] args) { return _this.Clone(); }

        public override InternalHandle Call(string functionName, InternalHandle _this, params InternalHandle[] args)
        {
            if (functionName.IsNullOrWhiteSpace()) throw new ArgumentNullException("functionName (cannot be null, empty, or only whitespace)");
            return _this.Clone();
        }*/

        // --------------------------------------------------------------------------------------------------------------------

        public bool Equals(InternalHandle jsOther)
        {
            if (jsOther.IsNull)
                return true;

            return false;
        }

        public bool Equals(DynamicJsNullV8 other)
        {
            if (ReferenceEquals(null, other))
            {
                return false;
            }

            return true;
        }
    }
}
