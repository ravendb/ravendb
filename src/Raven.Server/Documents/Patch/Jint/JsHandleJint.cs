using System;
using System.Runtime.CompilerServices;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Jint;
using Jint.Native;
using Jint.Native.Boolean;
using Jint.Native.Number;
using Jint.Native.Date;
using Jint.Native.Symbol;
using Jint.Native.String;
using Jint.Native.RegExp;
using Jint.Native.Array;
using Jint.Native.Function;
using Jint.Native.Object;
using Jint.Runtime.Interop;
using JintTypes = Jint.Runtime.Types;
using Raven.Server.Extensions.Jint;
using V8.Net;

namespace Raven.Server.Documents.Patch
{
    public struct JsHandleJint : IJsHandle<JsHandle>
    {
        private JsValue _item;
        private ObjectInstance _obj;

        public JsHandleJint(JsValue value)
        {
            _item = value;
            _obj = _item as ObjectInstance;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose()
        {
            Item = null;
        }

        public JsValue Item
        {
            get => _item;
            private set
            {
                _item = value;
                _obj = _item as ObjectInstance;
            }
        }
        
        public ObjectInstance Obj { get => _obj; }

        public JsHandle Clone()
        {
            return new JsHandle(this.Item);
        }

        public JsHandle Set(JsHandle value)
        {
            Item = value.Jint.Item;
            return new JsHandle(Item);
        }
        
        public void ThrowOnEmpty()
        {
            if (IsEmpty)
                throw new NotSupportedException($"Jint handle is empty.");
        }


        public IJsEngineHandle Engine
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (_obj == null)
                    throw new NotSupportedException($"Engine property is not supported for non-object Jint value.");
                return _obj.Engine as IJsEngineHandle;
            }
        }

        public object NativeObject
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { ThrowOnEmpty();  return Item.AsObject(); }
        }

        public bool IsEmpty
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item == null; }
        }

        public bool IsUndefined
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return IsEmpty || Item.IsUndefined(); }
        }

        public bool IsNull
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return !IsEmpty && Item.IsNull(); }
        }
            
        public bool IsNumberEx
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return IsNumber; }
        }

        public bool IsNumberOrIntEx
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return IsNumber || IsInt32; }
        }

        public bool IsStringEx
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return IsString; }
        }

        public bool IsBoolean
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return !IsEmpty && Item.IsBoolean(); }
        }

        public bool IsInt32
        {
            get
            {
                // there is no access to needed members
                return false; //!IsEmpty && Item?.IsInteger(), //(uint) (!IsEmpty && Item?.Type & (InternalTypes.Number | InternalTypes.Integer)) > 0U,
            }
        }

        public bool IsNumber
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return !IsEmpty && Item.IsNumber(); }
        }

        public bool IsString
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return !IsEmpty && Item.IsString(); }
        }

        public bool IsObject
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return !IsEmpty && Item.IsObject(); }
        }

        public bool IsFunction
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item is FunctionInstance; }
        }

        public bool IsDate
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return !IsEmpty && Item.IsDate(); }
        }

        public bool IsArray
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return !IsEmpty && Item.IsArray(); }
        }

        public bool IsRegExp
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return !IsEmpty && Item.IsRegExp(); }
        }

        public bool IsObjectType
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return !IsEmpty && Item.IsObject() || IsArray || IsDate || IsRegExp; } // TODO [shlomo] check if this is correct: BoolObject, NumberObject, StringObject 
        }

        public bool IsError
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return false; } // TODO [shlomo] check if this is correct
        }

        public bool AsBoolean
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { ThrowOnEmpty(); return Item.AsBoolean(); }
        }

        public int AsInt32
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { ThrowOnEmpty(); return (int)Item.AsNumber(); }
        }

        public double AsDouble
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { ThrowOnEmpty();  return Item.AsNumber(); }
        }

        public string AsString
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { ThrowOnEmpty();  return Item.AsString(); }
        }

        public DateTime AsDate
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { ThrowOnEmpty();  return Item.AsDate().ToDateTime(); }
        }
        
        public JSValueType ValueType
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (IsEmpty || Item.Type == JintTypes.None)
                    return JSValueType.Uninitialized;
                if (Item.IsUndefined())
                    return JSValueType.Undefined;
                if (Item.IsNull())
                    return JSValueType.Null;
                if (Item.IsBoolean())
                    return JSValueType.Bool;
                if (Item.IsNumber())
                    return JSValueType.Number;
                if (Item.IsDate())
                    return JSValueType.Date;
                if (Item.IsSymbol())
                    return JSValueType.String;
                if (Item.IsString())
                    return JSValueType.String;
                if (Item.IsRegExp())
                    return JSValueType.RegExp;
                if (Item.IsArray())
                    return JSValueType.Array;

                if (Item.IsObject())
                {
                    ObjectInstance obj = Item?.AsObject();
                    if (obj is BooleanInstance)
                        return JSValueType.BoolObject;
                    if (obj is NumberInstance)
                        return JSValueType.NumberObject;
                    if (obj is DateInstance)
                        return JSValueType.Date;
                    if (obj is SymbolInstance)
                        return JSValueType.StringObject;
                    if (obj is StringInstance)
                        return JSValueType.StringObject;
                    if (obj is RegExpInstance)
                        return JSValueType.RegExp;
                    if (obj is ArrayInstance)
                        return JSValueType.Array;
                    if (obj is IObjectWrapper)
                        return JSValueType.Object;
                    return JSValueType.Object;
                }

                throw new NotSupportedException($"Not supported jint type '{Item.Type}'.");
            }
        }

        public object Object
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (!IsObject)
                    return null;

                var objWrapper = _obj as ObjectWrapper;
                return objWrapper?.Target ?? _obj;
            }
        }
        
        public uint ArrayLength
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { ThrowOnEmpty();  return Item.AsArray().Length; }
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ThrowOnError()
        {}

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandle GetOwnProperty(string name)
        {
            if (_obj == null)
                throw new NotSupportedException($"Not supported for non object value.");

            return new JsHandle(_obj.GetOwnProperty(name).Value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandle GetOwnProperty(Int32 index)
        {
            if (_obj == null)
                throw new NotSupportedException($"Not supported for non object value.");

            return new JsHandle(_obj.GetOwnProperty(index).Value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool HasOwnProperty (string name)
        {
            if (_obj == null)
                throw new NotSupportedException($"Not supported for non object value.");

            return _obj.HasOwnProperty(name);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool HasProperty (string name)
        {
            if (_obj == null)
                throw new NotSupportedException($"Not supported for non object value.");

            return _obj.HasProperty(name);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void FastAddProperty(string name, JsHandle value, bool writable, bool enumerable, bool configurable)
        {
            if (_obj == null)
                throw new NotSupportedException($"Not supported for non object value.");

            _obj.FastAddProperty(name, value.Jint.Item, writable, enumerable, configurable);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SetProperty(string name, JsHandle value, bool throwOnError = false)
        {
            if (_obj == null)
                throw new NotSupportedException($"Not supported for non object value.");

            return _obj.Set(name, value.Jint.Item, throwOnError);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SetProperty(int index, JsHandle value, bool throwOnError = false)
        {
            if (_obj == null)
                throw new NotSupportedException($"Not supported for non object value.");

            return _obj.Set(index, value.Jint.Item, throwOnError);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetValue(string propertyName, out JsHandle value)
        {
            if (_obj == null)
                throw new NotSupportedException($"Not supported for non object value.");

            var res = _obj.TryGetValue(propertyName, out JsValue jsValue);
            value = new JsHandle(jsValue);
            return res;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandle GetProperty(string name)
        {
            if (_obj == null)
                throw new NotSupportedException($"Not supported for non object value.");

            return new JsHandle(_obj.GetOwnProperty(name).Value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandle GetProperty(int index)
        {
            if (_obj == null)
                throw new NotSupportedException($"Not supported for non object value.");

            return new JsHandle(_obj.GetOwnProperty(index).Value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool DeleteProperty(string name, bool throwOnError = false)
        {
            if (_obj == null)
                throw new NotSupportedException($"Not supported for non object value.");

            var res = true;
            if (throwOnError)
                _obj.DeletePropertyOrThrow(name);
            else
                res = _obj.Delete(name);
            return res;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool DeleteProperty(int index, bool throwOnError = false)
        {
            if (_obj == null)
                throw new NotSupportedException($"Not supported for non object value.");

            var res = true;
            if (throwOnError)
                _obj.DeletePropertyOrThrow(index);
            else
                res = _obj.Delete(index);
            return res;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string[] GetPropertyNames()
        {
            return GetOwnPropertyNames();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string[] GetOwnPropertyNames()
        {
            if (_obj == null)
                throw new NotSupportedException($"Not supported for non object value.");

            var jsKeys = _obj.GetOwnPropertyKeys();
            int arrayLength = jsKeys.Count;
            var res = new string[arrayLength];
            for (int i = 0; i < arrayLength; ++i)
            {
                res[i] = jsKeys[i].AsString();
            }

            return res;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerable<KeyValuePair<string, JsHandle>> GetOwnProperties()
        {
            if (_obj == null)
                throw new NotSupportedException($"Not supported for non object value.");

            foreach (var kvp in _obj.GetOwnProperties())
            {
                yield return new KeyValuePair<string, JsHandle>(kvp.Key.AsString(), new JsHandle(kvp.Value.Value));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerable<KeyValuePair<string, JsHandle>> GetProperties()
        {
            return GetOwnProperties();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandle Call(string functionName, JsHandle _this, params JsHandle[] args)
        {
            if (_obj == null)
                throw new NotSupportedException($"Not supported for non object value.");

            int arrayLength = args.Length;
            var jsArgs = new JsValue[arrayLength];
            for (int i = 0; i < arrayLength; ++i)
            {
                jsArgs[i] = args[i].Jint.Item;
            }

            return new JsHandle(_obj.Call(functionName, _this.Jint.Item, jsArgs));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandle StaticCall(string functionName, params JsHandle[] args)
        {
            return Call(functionName, new JsHandle(JsValue.Null), args);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandle Call(JsHandle _this, params JsHandle[] args)
        {
            if (_obj == null)
                throw new NotSupportedException($"Not supported for non object value.");

            int arrayLength = args.Length;
            var jsArgs = new JsValue[arrayLength];
            for (int i = 0; i < arrayLength; ++i)
            {
                jsArgs[i] = args[i].Jint.Item;
            }

            return new JsHandle(_obj.Call(_this.Jint.Item, jsArgs));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandle StaticCall(params JsHandle[] args)
        {
            return Call(new JsHandle(JsValue.Null), args);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(JsHandle other)
        {
            return Item.Equals(other.Jint.Item);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool Equals(object obj)
        {
            return obj is JsHandle other && Equals(other);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override int GetHashCode()
        {
            return  HashCode.Combine((int) JsHandleType.Jint, Item);
        }
    }
}
