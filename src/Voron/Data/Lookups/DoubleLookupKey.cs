using System;
using System.Diagnostics.Contracts;

namespace Voron.Data.Lookups;

public struct DoubleLookupKey : ILookupKey
{
    public double Value;
    
    public long ToLong()
    {
        return BitConverter.DoubleToInt64Bits(Value);
    }
    
    public static implicit operator DoubleLookupKey(double d)
    {
        return new DoubleLookupKey(d);
    }

    public DoubleLookupKey(double value)
    {
        Value = value;
    }

    public override string ToString()
    {
        return Value.ToString();
    }

    public static T FromLong<T>(long l)
    {
        if (typeof(T) != typeof(DoubleLookupKey))
        {
            throw new NotSupportedException(typeof(T).FullName);
        }

        return (T)(object)new DoubleLookupKey(BitConverter.Int64BitsToDouble(l));
    }

    
    public static long MinValue => BitConverter.DoubleToInt64Bits(double.MinValue);

    [Pure]
    public int CompareTo<T>(Lookup<T> parent, long l) where T : struct, ILookupKey
    {
        var d = BitConverter.Int64BitsToDouble(l);
        return Value.CompareTo(d);
    }

    [Pure]
    public bool IsEqual<T>(T k) where T : ILookupKey
    {
        if (typeof(T) != typeof(DoubleLookupKey))
        {
            throw new NotSupportedException(typeof(T).FullName);
        }

        var o = (DoubleLookupKey)(object)k;
        return Math.Abs(Value - o.Value) < double.Epsilon;
    }

    public void OnNewKeyAddition<T>(Lookup<T> parent) where T : struct, ILookupKey
    {
        
    }

    public void OnKeyRemoval<T>(Lookup<T> parent) where T : struct, ILookupKey
    {
    }

    public int CompareTo<T>(T l) where T : ILookupKey
    {
        if (typeof(T) != typeof(DoubleLookupKey))
        {
            throw new NotSupportedException(typeof(T).FullName);
        }

        var o = (DoubleLookupKey)(object)l;
        return Value.CompareTo(o.Value);
    }
}
