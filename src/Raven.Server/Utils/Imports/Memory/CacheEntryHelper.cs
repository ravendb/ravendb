using System.Diagnostics;
using System.Threading;

namespace Raven.Server.Utils.Imports.Memory;

internal static class CacheEntryHelper
{
    private static readonly AsyncLocal<CacheEntry> _current = new AsyncLocal<CacheEntry>();

    internal static CacheEntry Current
    {
        get => _current.Value;
        private set => _current.Value = value;
    }

    internal static CacheEntry EnterScope(CacheEntry current)
    {
        CacheEntry previous = Current;
        Current = current;
        return previous;
    }

    internal static void ExitScope(CacheEntry current, CacheEntry previous)
    {
        Debug.Assert(Current == current, "Entries disposed in invalid order");
        Current = previous;
    }
}
