using System;
using System.Collections.Generic;
using V8.Net;

namespace Raven.Server.Documents.Patch;

public enum JsHandleType : byte
{
    Empty = 0,
    V8 = 1,
    Jint = 2,
    JintError = 3
}

public interface IJsHandle<T> : IDisposable, IClonable<T>, IEquatable<T>
    where T : IJsHandle<T>/*, new()*/
{
    T Set(T value);

    //TODO: egor clear debug code
    //public static T Empty;

    //public static T GetEmpty()
    //{
    //    if (Empty == null)
    //    {
    //        //Empty = new T();
    //    }

    //    return Empty;
    //}

    public T GetEmpty2();

 //   IJsEngineHandle<T> Engine { get; set; }

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

    int ArrayLength { get; }

    void ThrowOnError();

    bool HasOwnProperty(string name);

    bool HasProperty(string name);

    void FastAddProperty(string name, T value, bool writable, bool enumerable, bool configurable);

    bool SetProperty(string name, T value, bool throwOnError = false);

    bool SetProperty(int index, T value, bool throwOnError = false);

    bool TryGetValue(string propertyName, out T value);
    bool IsBinder();
    T GetOwnProperty(string name);

    T GetOwnProperty(Int32 index);

    T GetProperty(string name);

    T GetProperty(int index);

    bool DeleteProperty(string name, bool throwOnError = false);

    bool DeleteProperty(int index, bool throwOnError = false);

    string[] GetPropertyNames();

    string[] GetOwnPropertyNames();

    IEnumerable<KeyValuePair<string, T>> GetOwnProperties();

    IEnumerable<KeyValuePair<string, T>> GetProperties();

    T Call(string functionName, T _this, params T[] args);

    T StaticCall(string functionName, params T[] args);

    T Call(T _this, params T[] args);
    T StaticCall(params T[] args);
    object AsObject();

    //TODO: egor c# 10 https://docs.microsoft.com/en-us/dotnet/csharp/whats-new/tutorials/static-abstract-interface-methods
    //public static abstract bool operator !=(IJsHandle<T> lhs, IJsHandle<T> rhs);
    //public static abstract bool operator ==(IJsHandle<T> lhs, IJsHandle<T> rhs);
}
