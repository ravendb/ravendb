using System;
using System.Runtime.CompilerServices;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using V8.Net;

namespace Raven.Server.Documents.Patch
{
    public struct JsHandleV8 : IJsHandle<JsHandle>
    {
        public InternalHandle Item;

        public JsHandleV8(ref InternalHandle value)
        {
            Item = value;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose()
        {
            Item.Dispose();
        }

        public JsHandle Clone()
        {
            var ItemClone = new InternalHandle(ref Item, true);
            return new JsHandle(ItemClone);
        }

        public JsHandle Set(JsHandle value)
        {
            return new JsHandle(Item.Set(value.V8.Item));
        }

        public IJsEngineHandle Engine
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.Engine as IJsEngineHandle; }
        }

        public object NativeObject
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.Object; }
        }

        public bool IsEmpty
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.IsEmpty; }
        }

        public bool IsUndefined
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.IsUndefined; }
        }

        public bool IsNull
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.IsNull; }
        }
        
        public bool IsNumberEx
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.IsNumberEx; }
        }

        public bool IsNumberOrIntEx
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.IsNumberOrIntEx; }
        }

        public bool IsStringEx
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.IsStringEx; }
        }

        public bool IsBoolean
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.IsBoolean; }
        }

        public bool IsInt32
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.IsInt32; }
        }

        public bool IsNumber
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.IsNumber; }
        }

        public bool IsString
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.IsString; }
        }

        public bool IsObject
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.IsObject; }
        }
        
        public bool IsFunction
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.IsFunction; }
        }

        public bool IsDate
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.IsDate; }
        }

        public bool IsArray
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.IsArray; }
        }

        public bool IsRegExp
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.IsRegExp; }
        }

        public bool IsObjectType
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.IsObjectType; }
        }

        public bool IsError
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.IsError; }
        }

        public bool AsBoolean
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.AsBoolean; }
        }

        public int AsInt32
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.AsInt32; }
        }

        public double AsDouble
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.AsDouble; }
        }

        public string AsString
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.AsString; }
        }

        public DateTime AsDate
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.AsDate; }
        }
        
        public JSValueType ValueType
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.ValueType; }
        }

        public object Object
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.BoundObject ?? Item.Object; }
        }
        
        public uint ArrayLength
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (uint)Item.ArrayLength; }
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ThrowOnError()
        {
            Item.ThrowOnError();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool HasOwnProperty(string name)
        {
            return Item.HasOwnProperty(name);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool HasProperty(string name)
        {
            return Item.HasProperty(name);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void FastAddProperty(string name, JsHandle value, bool writable, bool enumerable, bool configurable)
        {
            Item.FastAddProperty(name, value.V8.Item, writable, enumerable, configurable);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetPropertyOrThrow(string propertyName, JsHandle value)
        {
            Item.SetPropertyOrThrow(propertyName, value.V8.Item);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SetProperty(string name, JsHandle value, bool throwOnError = false)
        {
            var res = Item.SetProperty(name, value.V8.Item);
            if (!res && throwOnError)
                throw new InvalidOperationException($"Failed to set property {name}");
            return res;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SetProperty(int index, JsHandle value, bool throwOnError = false)
        {
            var res = Item.SetProperty(index, value.V8.Item);
            if (!res && throwOnError)
                throw new InvalidOperationException($"Failed to set property {index}");
            return res;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetValue(string propertyName, out JsHandle value)
        {
            InternalHandle jsValue;
            bool res = Item.TryGetValue(propertyName, out jsValue);
            value = new JsHandle(jsValue);
            return res;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandle GetOwnProperty(string name)
        {
            return new JsHandle(Item.GetOwnProperty(name));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandle GetOwnProperty(Int32 index)
        {
            return new JsHandle(Item.GetOwnProperty(index));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandle GetProperty(string name)
        {
            return new JsHandle(Item.GetProperty(name));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandle GetProperty(int index)
        {
            return new JsHandle(Item.GetProperty(index));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DeletePropertyOrThrow(string propertyName)
        {
            Item.DeletePropertyOrThrow(propertyName);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool DeleteProperty(string name, bool throwOnError = false)
        {
            var res = Item.DeleteProperty(name);
            if (!res && throwOnError)
                throw new InvalidOperationException($"Failed to delete property {name}");
            return res;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool DeleteProperty(int index, bool throwOnError = false)
        {
            var res = Item.DeleteProperty(index);
            if (!res && throwOnError)
                throw new InvalidOperationException($"Failed to delete property {index}");
            return res;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string[] GetPropertyNames()
        {
            return Item.GetPropertyNames();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string[] GetOwnPropertyNames()
        {
            return Item.GetOwnPropertyNames();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerable<KeyValuePair<string, JsHandle>> GetOwnProperties()
        {
            foreach (var kvp in Item.GetOwnProperties())
            {
                yield return new KeyValuePair<string, JsHandle>(kvp.Key, new JsHandle(kvp.Value));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerable<KeyValuePair<string, JsHandle>> GetProperties()
        {
            foreach (var kvp in Item.GetProperties())
            {
                yield return new KeyValuePair<string, JsHandle>(kvp.Key, new JsHandle(kvp.Value));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandle Call(string functionName, JsHandle _this, params JsHandle[] args)
        {
            int arrayLength = args.Length;
            var jsArgs = new InternalHandle[arrayLength];
            for (int i = 0; i < arrayLength; ++i)
            {
                jsArgs[i] = args[i].V8.Item;
            }

            return new JsHandle(Item.Call(functionName, _this.V8.Item, jsArgs));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandle StaticCall(string functionName, params JsHandle[] args)
        {
            int arrayLength = args.Length;
            var jsArgs = new InternalHandle[arrayLength];
            for (int i = 0; i < arrayLength; ++i)
            {
                jsArgs[i] = args[i].V8.Item;
            }

            return new JsHandle(Item.StaticCall(functionName, jsArgs));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandle Call(JsHandle _this, params JsHandle[] args)
        {
            int arrayLength = args.Length;
            var jsArgs = new InternalHandle[arrayLength];
            for (int i = 0; i < arrayLength; ++i)
            {
                jsArgs[i] = args[i].V8.Item;
            }

            return new JsHandle(Item.Call(_this.V8.Item, jsArgs));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandle StaticCall(params JsHandle[] args)
        {
            int arrayLength = args.Length;
            var jsArgs = new InternalHandle[arrayLength];
            for (int i = 0; i < arrayLength; ++i)
            {
                jsArgs[i] = args[i].V8.Item;
            }

            return new JsHandle(Item.StaticCall(jsArgs));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(JsHandle other)
        {
            return Item.Equals(other.V8.Item);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool Equals(object obj)
        {
            return obj is JsHandle other && Equals(other);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override int GetHashCode()
        {
            return  HashCode.Combine((int) JsHandleType.V8, Item);
        }
    }
}
