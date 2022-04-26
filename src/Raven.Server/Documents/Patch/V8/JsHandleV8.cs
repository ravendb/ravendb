using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using V8.Net;

namespace Raven.Server.Documents.Patch.V8
{

    public static class JsHandleV8Extensions
    {
        //public static JsHandleV8[] ToJsHandle1(this InternalHandle handle)
        //{
        //    return new[] { new JsHandleV8(ref handle) };
        //}

        public static JsHandleV8[] ToJsHandleArray(this InternalHandle[] handle)
        {
            var arr = new JsHandleV8[handle.Length];
            for (var index = 0; index < handle.Length; index++)
            {
                var x = handle[index];
                arr[index] = new JsHandleV8(ref x);
            }

            return arr;
        }
    }

    public struct JsHandleV8 : IJsHandle<JsHandleV8>
    {
        public static JsHandleV8 Empty = new JsHandleV8() { Item = InternalHandle.Empty };
        public static JsHandleV8 Null = new JsHandleV8() { Item = null };
        // public static JsHandleV8 Undefined = Empty; // valuetype.undefined
        //  public static JsHandleV8 Undefined2 = new JsHandleV8() { Item = new InternalHandle() };

        public InternalHandle Item;

        //public JsHandleV8()
        //{

        //}

        public JsHandleV8(ref InternalHandle value)
        {
            Item = value;
        }

        public object AsObject()
        {
            return Item.BoundObject;
        }

        public bool IsBinder()
        {
            return Item.IsBinder;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose()
        {
            Item.Dispose();
        }

        public JsHandleV8 Clone()
        {
            var cloned = Item.Clone();
            return new JsHandleV8(ref cloned);
        }

        public JsHandleV8 Set(JsHandleV8 value)
        {
            return new JsHandleV8(ref value.Item);
        }

        public JsHandleV8 Empty1()
        {
            throw new NotImplementedException();
        }


        //TODO: egor
        //public IJsEngineHandle Engine
        //{
        //    [MethodImpl(MethodImplOptions.AggressiveInlining)]



        //    get { return (IJsEngineHandle)Item.Engine as IJsEngineHandle; }
        //}

        public JsHandleV8 GetEmpty2()
        {
            return Empty;
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

        public int ArrayLength
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Item.ArrayLength; }
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
        public void FastAddProperty(string name, JsHandleV8 value, bool writable, bool enumerable, bool configurable)
        {
            Item.FastAddProperty(name, value.Item, writable, enumerable, configurable);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetPropertyOrThrow(string propertyName, JsHandleV8 value)
        {
            Item.SetPropertyOrThrow(propertyName, value.Item);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SetProperty(string name, JsHandleV8 value, bool throwOnError = false)
        {
            var res = Item.SetProperty(name, value.Item);
            if (!res && throwOnError)
                throw new InvalidOperationException($"Failed to set property {name}");
            return res;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SetProperty(int index, JsHandleV8 value, bool throwOnError = false)
        {
            var res = Item.SetProperty(index, value.Item);
            if (!res && throwOnError)
                throw new InvalidOperationException($"Failed to set property {index}");
            return res;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetValue(string propertyName, out JsHandleV8 value)
        {
            bool res = Item.TryGetValue(propertyName, out var jsValue);
            value = new JsHandleV8(ref jsValue);
            return res;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandleV8 GetOwnProperty(string name)
        {
            var prop = Item.GetOwnProperty(name);
            return new JsHandleV8(ref prop);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandleV8 GetOwnProperty(int index)
        {
            var prop = Item.GetOwnProperty(index);
            return new JsHandleV8(ref prop);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandleV8 GetProperty(string name)
        {
            var prop = Item.GetProperty(name);
            return new JsHandleV8(ref prop);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandleV8 GetProperty(int index)
        {
            var prop = Item.GetProperty(index);
            return new JsHandleV8(ref prop);
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
        public IEnumerable<KeyValuePair<string, JsHandleV8>> GetOwnProperties()
        {
            foreach (var kvp in Item.GetOwnProperties())
            {
                var prop = kvp.Value;
                yield return new KeyValuePair<string, JsHandleV8>(kvp.Key, new JsHandleV8(ref prop));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerable<KeyValuePair<string, JsHandleV8>> GetProperties()
        {
            foreach (var kvp in Item.GetProperties())
            {
                var prop = kvp.Value;

                yield return new KeyValuePair<string, JsHandleV8>(kvp.Key, new JsHandleV8(ref prop));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandleV8 Call(string functionName, JsHandleV8 _this, params JsHandleV8[] args)
        {
            int arrayLength = args.Length;
            var jsArgs = new InternalHandle[arrayLength];
            for (int i = 0; i < arrayLength; ++i)
            {
                jsArgs[i] = args[i].Item;
            }

            var res = Item.Call(functionName, _this.Item, jsArgs);
            return new JsHandleV8(ref res);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandleV8 StaticCall(string functionName, params JsHandleV8[] args)
        {
            int arrayLength = args.Length;
            var jsArgs = new InternalHandle[arrayLength];
            for (int i = 0; i < arrayLength; ++i)
            {
                jsArgs[i] = args[i].Item;
            }

            var res = Item.StaticCall(functionName, jsArgs);
            return new JsHandleV8(ref res);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandleV8 Call(JsHandleV8 _this, params JsHandleV8[] args)
        {
            int arrayLength = args.Length;
            var jsArgs = new InternalHandle[arrayLength];
            for (int i = 0; i < arrayLength; ++i)
            {
                jsArgs[i] = args[i].Item;
            }

            var res = Item.Call(_this.Item, jsArgs);
            return new JsHandleV8(ref res);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandleV8 StaticCall(params JsHandleV8[] args)
        {
            int arrayLength = args.Length;
            var jsArgs = new InternalHandle[arrayLength];
            for (int i = 0; i < arrayLength; ++i)
            {
                jsArgs[i] = args[i].Item;
            }

            var res = Item.StaticCall(jsArgs);
            return new JsHandleV8(ref res);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(JsHandle other)
        {
            return Item.Equals(other.V8.Item);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool Equals(object obj)
        {
            return obj is JsHandleV8 other && Equals(other);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override int GetHashCode()
        {
            return Item.GetHashCode();
        }

        public bool Equals(JsHandleV8 other)
        {
            return Item.Equals(other.Item);
        }
    }
}
