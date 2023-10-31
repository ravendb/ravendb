using System.Diagnostics.Contracts;

namespace Voron.Data.Lookups;

public interface ILookupKey
{
    void Reset();
    long ToLong();

    static abstract T FromLong<T>(long l);

    static abstract long MinValue { get; }
    
    void Init<T>(Lookup<T> parent) where T : struct, ILookupKey;

    int CompareTo<T>(Lookup<T> parent, long l) where T : struct, ILookupKey;
    
    [Pure]
    int CompareTo<T>(T l) where T : ILookupKey;

    [Pure]
    bool IsEqual<T>(T k) where T : ILookupKey;

    void OnNewKeyAddition<T>(Lookup<T> parent) where T : struct, ILookupKey;

    void OnKeyRemoval<T>(Lookup<T> parent) where T : struct, ILookupKey;
    
    void PreventTermRemoval<T>(Lookup<T> parent) where T : struct, ILookupKey;
    
    string ToString<T>(Lookup<T> parent) where T : struct, ILookupKey;
}
