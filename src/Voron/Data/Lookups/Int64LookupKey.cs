using System;
using System.Diagnostics.Contracts;

namespace Voron.Data.Lookups;

public struct Int64LookupKey : ILookupKey
{
    public long Value;
    
    public long ToLong()
    {
        return Value;
    }

    public static implicit operator Int64LookupKey(long v)
    {
        return new Int64LookupKey(v);
    }
    
    public static T FromLong<T>(long l)
    {
        if (typeof(T) != typeof(Int64LookupKey))
        {
            throw new NotSupportedException(typeof(T).FullName);
        }

        return (T)(object)new Int64LookupKey(l);
    }

    public static long MinValue => long.MinValue;

    public int CompareTo<T>(Lookup<T> parent, long l) where T : struct, ILookupKey
    {
        return Value.CompareTo(l);
    }

    [Pure]
    public int CompareTo<T>(T l) where T : ILookupKey
    {
        if (typeof(T) != typeof(Int64LookupKey))
        {
            throw new NotSupportedException(typeof(T).FullName);
        }

        var o = (Int64LookupKey)(object)l;
        return Value.CompareTo(o.Value);
    }

    [Pure]
    public bool IsEqual<T>(T k) where T : ILookupKey
    {
        if (typeof(T) != typeof(Int64LookupKey))
        {
            throw new NotSupportedException(typeof(T).FullName);
        }

        var o = (Int64LookupKey)(object)k;
        return Value == o.Value;
    }

    public void OnNewKeyAddition<T>(Lookup<T> parent) where T : struct, ILookupKey
    {
        
    }

    public void OnKeyRemoval<T>(Lookup<T> parent) where T : struct, ILookupKey
    {
    }

    public Int64LookupKey(long value)
    {
        Value = value;
    }

    public override string ToString()
    {
        return Value.ToString();
    }
}
