using System;
using System.Runtime.InteropServices;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime;
using Jint.Runtime.Interop;
using V8.Net;
using Raven.Server.Extensions.V8;
using Raven.Server.Extensions.Jint;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch
{
    public enum JsHandleType : byte
    {
        Empty = 0,
        V8 = 1,
        Jint = 2,
        JintError = 3
    }

    public interface IJsHandle<THost> : IDisposable, IClonable<THost>, IEquatable<THost>
    {
        JsHandle Set(JsHandle value);
        
        IJsEngineHandle Engine { get; }
        
        object NativeObject { get; }

        bool IsEmpty { get; }

        bool IsUndefined { get; }

        bool IsNull { get; }

        bool IsNumberEx { get; }

        bool IsNumberOrIntEx { get; }

        bool IsStringEx { get; }

        bool IsBoolean { get; }

        bool IsInt32 { get; }

        bool IsNumber { get; }

        bool IsString { get; }

        bool IsObject { get; }
        
        bool IsFunction { get; }

        bool IsDate { get; }

        bool IsArray { get; }

        bool IsRegExp { get; }

        bool IsObjectType { get; }

        bool IsError { get; }

        bool AsBoolean { get; }

        int AsInt32 { get; }

        double AsDouble { get; }

        string AsString { get; }

        DateTime AsDate { get; }

        
        JSValueType ValueType { get; }

        object Object { get; }

        uint ArrayLength { get; }
        
        void ThrowOnError();

        bool HasOwnProperty (string name);

        bool HasProperty (string name);

        void FastAddProperty(string name, JsHandle value, bool writable, bool enumerable, bool configurable);
        
        bool SetProperty(string name, JsHandle value, bool throwOnError = false);

        bool SetProperty(int index, JsHandle value, bool throwOnError = false);

        bool TryGetValue(string propertyName, out JsHandle value);

        JsHandle GetOwnProperty(string name);

        JsHandle GetOwnProperty(Int32 index);

        JsHandle GetProperty(string name);

        JsHandle GetProperty(int index);

        bool DeleteProperty(string name, bool throwOnError = false);

        bool DeleteProperty(int index, bool throwOnError = false);

        string[] GetPropertyNames();

        string[] GetOwnPropertyNames();

        IEnumerable<KeyValuePair<string, JsHandle>> GetOwnProperties();

        IEnumerable<KeyValuePair<string, JsHandle>> GetProperties();

        JsHandle Call(string functionName, JsHandle _this, params JsHandle[] args);

        JsHandle StaticCall(string functionName, params JsHandle[] args);

        JsHandle Call(JsHandle _this, params JsHandle[] args);

        JsHandle StaticCall(params JsHandle[] args);
    }
}
