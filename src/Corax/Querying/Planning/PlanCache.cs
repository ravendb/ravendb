using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Threading;

namespace Corax.Querying.Planning;

/// <summary>
/// Caches compiled query plans per index instance.
/// Lives on IndexSearcher — GC'd when the index is replaced.
///
/// Two-generation structure: a single atomic <see cref="CacheRecord"/> reference
/// holds both the current and previous ConcurrentDictionaries. Rotation swaps the
/// entire generation atomically.
///
/// Per-query: fixed-capacity SoA (struct-of-arrays, default 32 slots) — a ushort[] holding
/// a 16-bit pre-filter slice of each plan's <see cref="PlanCacheKeyHash"/> plus a CompiledPlan[]
/// for the payloads. SIMD compares scan all slots in Vector256/Vector128 iterations over 16-bit
/// lanes (16 slots per Vector256 step); a lane hit is confirmed with a full 256-bit digest
/// compare.
/// </summary>
public class PlanCache
{
    private int MaxPlansPerQuery { get; }
    private int HalfOfMaxDistinctQueries { get; }

    private static long GlobalGenerationIndex;

    private long _generationIdx = Interlocked.Increment(ref GlobalGenerationIndex);

    public long GenerationIdx => _generationIdx;

    public void TouchGeneration()
    {
        // used to invalidate PlanMemo when FieldsWithMultipleTerms changes, so we'll recompute the plans for those queries
        Volatile.Write(ref _generationIdx, Interlocked.Increment(ref GlobalGenerationIndex));
    }

    private sealed record CacheRecord(
        ConcurrentDictionary<Vector256<long>, PerQueryPlans> Current,
        ConcurrentDictionary<Vector256<long>, PerQueryPlans> Previous);

    private CacheRecord _cache;

    public PlanCache(int maxPlansPerQuery = 32, int halfOfMaxDistinctQueries = 2048)
    {
        maxPlansPerQuery = (maxPlansPerQuery + 15) & ~15; // 16 aligned - Vector256<ushort> loop can never read past the end of the array
        MaxPlansPerQuery = maxPlansPerQuery;
        HalfOfMaxDistinctQueries = Math.Max(16, halfOfMaxDistinctQueries / 2);
        _cache = new CacheRecord([], []);
    }

    public PerQueryPlans GetBucket(in Vector256<long> structuralKey)
    {
        var gen = _cache;
        if (gen.Current.TryGetValue(structuralKey, out var per) is false)
            gen.Previous.TryGetValue(structuralKey, out per);

        return per;
    }

    /// <summary>The caller publishes compiled plan variants into the returned bucket. May trigger generational swap if exceeded cache limits.</summary>
    public PerQueryPlans GetOrAddBucket(in Vector256<long> structuralKey, PlanTemplate template, string queryText)
    {
        var gen = _cache;

        // This is called only after we called GetBucket (and didn't get a result), so we expect to add it below
        if (gen.Current.Count >= HalfOfMaxDistinctQueries)
        {
            // When the current generation reach half the max, rotate.
            var newGen = new CacheRecord([], gen.Current);
            // CompareExchange returns the previous value. If it equals gen, we won
            // the race and newGen is now installed. If another thread beat us, the
            // returned value is the generation _they_ installed — use that instead.
            var prev = Interlocked.CompareExchange(ref _cache, newGen, gen);
            gen = prev == gen ? newGen : prev!;
        }

        return gen.Current.GetOrAdd(structuralKey,
            static (_, arg) => new PerQueryPlans(arg.MaxPlansPerQuery, arg.template, arg.queryText),
            (MaxPlansPerQuery, template, queryText));
    }

    /// <summary>
    /// A single cached query text, its parse template, and every compiled plan variant
    /// currently held for it. Returned by <see cref="Snapshot"/>. Intended for diagnostics,
    /// introspection, and tooling — not on any hot path.
    /// </summary>
    public readonly record struct PlanCacheEntry(string QueryText, PlanTemplate Template, CompiledPlan[] Plans);

    /// <summary>
    /// Point-in-time snapshot of every cached query and its compiled plan variants across both
    /// generations. The current generation wins on duplicate structural keys. Reads are lock-free and
    /// may observe concurrent publishes, adequate for diagnostics.
    /// </summary>
    public IReadOnlyList<PlanCacheEntry> Snapshot()
    {
        var gen = _cache;
        var result = new List<PlanCacheEntry>();
        var seen = new HashSet<Vector256<long>>();

        foreach (var (key, per) in gen.Current)
        {
            if (seen.Add(key))
                result.Add(new PlanCacheEntry(per.QueryText, per.Template, per.SnapshotPlans()));
        }

        foreach (var (key, per) in gen.Previous)
        {
            if (seen.Add(key))
                result.Add(new PlanCacheEntry(per.QueryText, per.Template, per.SnapshotPlans()));
        }

        return result;
    }

    /// <summary>
    /// Fixed-slot per-query plan cache. Two parallel arrays (_hashLo, _plans) of maxSlots
    /// entries (default 32, must be a multiple of 16).
    ///
    /// Lookup: compare 16 slots per iteration, then confirm by comparing the plan's full 256-bit
    /// <see cref="PlanCacheKeyHash"/>. The digest is the complete plan identity, so we check that too.
    /// Collision chances are 1/64K, and we have 32 slots by default. Meaning the chance is ~0.75% for
    /// a collision (acceptable, since we'll check the full digest).
    /// </summary>
    public sealed class PerQueryPlans(int maxSlots, PlanTemplate template, string queryText)
    {
        private readonly ushort[] _hashLo = new ushort[maxSlots];
        private readonly CompiledPlan[] _plans = new CompiledPlan[maxSlots];

        /// <summary>
        /// The query text this bucket was first compiled for diagnostics only — surfaced by <see cref="Snapshot"/>.
        /// Queries are cached _structurally_ - same query with different literals use the same cache key, the first query text is held here.
        /// </summary>
        public readonly string QueryText = queryText;

        /// <summary>
        /// Monotonically increasing slot allocator. Counts from 0 up to maxSlots and
        /// then stays there. Once it reaches maxSlots, all subsequent publishes use random
        /// eviction (pick any slot).
        ///
        /// This is by design: a PerQueryPlans is expected to stabilize at maxSlots distinct plan variants for the
        /// lifetime of the IndexSearcher; past that point we accept random replacement as the steady state.
        /// </summary>
        private int _nextSlot;

        public readonly PlanTemplate Template = template;

        public CompiledPlan TryLookup(in Vector256<long> hash)
        {
            ushort key = PreFilterKey(hash);
            return Vec256Lookup(key, hash);
        }

        /// <summary>
        /// Hash bits are well-distributed, so any 16 give good coverage.
        /// Maps 0 to 1 so a populated slot's key never equals the default-zero value of an empty slot.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ushort PreFilterKey(in Vector256<long> hash)
        {
            ushort bits = (ushort)hash[0];
            return bits == 0 ? (ushort)1 : bits;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private CompiledPlan Confirm(int slot, in Vector256<long> hash)
        {
            // Already matched the 16-bit pre-filter; confirm the full 256-bit digest against the
            // plan's own embedded key. Volatile read guards against torn writes — the
            // _hashLo entry could be published before _plans[slot] in a concurrent Publish.
            var plan = Volatile.Read(ref _plans[slot]);
            return plan != null && plan.CacheKeyHash.Equals(hash) ? plan : null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private CompiledPlan Vec256Lookup(ushort key, in Vector256<long> hash)
        {
            // We use no platform specific code here, when running on hardware that doesn't have Vector256, the JIT will emit the right downgrade
            var keyVec = Vector256.Create(key);
            for (int i = 0; i < _hashLo.Length; i += Vector256<ushort>.Count)
            {
                var slots = Vector256.LoadUnsafe(ref _hashLo[i]);
                uint mask = Vector256.Equals(slots, keyVec).ExtractMostSignificantBits();
                while (mask != 0)
                {
                    int lane = BitOperations.TrailingZeroCount(mask);
                    mask &= mask - 1;
                    var resolved = Confirm(i + lane, hash);
                    if (resolved != null)
                        return resolved;
                }
            }
            return null;
        }

        public void Publish(CompiledPlan plan)
        {
            int slot;
            while (true)
            {
                int filled = Volatile.Read(ref _nextSlot);
                if (filled >= maxSlots)
                {
                    // Cache full — random eviction. _nextSlot stays at maxSlots
                    // permanently; see field doc for why this is intentional.
                    slot = Random.Shared.Next(0, maxSlots);
                    break;
                }

                if (Interlocked.CompareExchange(ref _nextSlot, filled + 1, filled) == filled)
                {
                    slot = filled;
                    break;
                }
            }

            // Publish the payload before the pre-filter key: a reader that observes the
            // matching _hashLo entry must be able to see the corresponding plan. The Confirm
            // step re-reads _plans[slot] volatile and re-checks the full digest, so a stale
            // key with a not-yet-written (or already-replaced) plan resolves to a miss.
            Volatile.Write(ref _plans[slot], plan);
            Volatile.Write(ref _hashLo[slot], PreFilterKey(plan.CacheKeyHash));
        }

        /// <summary>Lock-free snapshot of all non-null plan slots. Best-effort; see <see cref="Snapshot"/>.</summary>
        public CompiledPlan[] SnapshotPlans()
        {
            var list = new List<CompiledPlan>(_plans.Length);
            for (int i = 0; i < _plans.Length; i++)
            {
                var plan = Volatile.Read(ref _plans[i]);
                if (plan != null)
                    list.Add(plan);
            }
            return list.ToArray();
        }
    }
}
