using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Lucene.Net.Support;
using Raven.Server.Documents.Replication;
using Sparrow.Utils;
using SparrowChangeVectorPart = Sparrow.Utils.ChangeVectorPart;

namespace Raven.Server.Utils;

internal static class ChangeVectorMerger
{
    private enum State
    {
        Tag,
        Etag,
        Whitespace
    }

    private enum MergeMode
    {
        Max,
        Min
    }

    [ThreadStatic] private static List<ChangeVectorEntry> MergeVectorBuffer;
    [ThreadStatic] private static List<ChangeVectorEntry> MergeVectorVersionBuffer;

    static ChangeVectorMerger()
    {
        ThreadLocalCleanup.ReleaseThreadLocalState += () =>
        {
            MergeVectorBuffer = null;
            MergeVectorVersionBuffer = null;
        };
    }

    /// <summary>
    /// Merges change vectors by taking the maximum etag per database id. If any input is composite, order and version are merged separately.
    /// </summary>
    /// <example>
    /// <code>
    /// [A:10-dbA] + [A:12-dbA]                       => A:12-dbA
    /// [A:10-dbA] + [B:20-dbB] + [A:12-dbA]          => A:12-dbA, B:20-dbB
    /// [S1:500-dbS|A:10-dbA] + [S2:700-dbT|A:12-dbA] => S1:500-dbS, S2:700-dbT|A:12-dbA
    /// [S1:500-dbS|A:10-dbA] + [A:12-dbA]            => A:12-dbA, S1:500-dbS|A:12-dbA
    /// </code>
    /// </example>
    public static string Merge(params ReadOnlySpan<string> changeVectors)
    {
        if (changeVectors.Length == 2)
        {
            if (string.IsNullOrEmpty(changeVectors[0]))
                return changeVectors[1];
            if (string.IsNullOrEmpty(changeVectors[1]))
                return changeVectors[0];
        }

        if (ChangeVectorParts.HasComposite(changeVectors) == false)
        {
            var buffer = MergeVectorBuffer ??= new EquatableList<ChangeVectorEntry>();
            buffer.Clear();

            foreach (var changeVector in changeVectors)
            {
                if (string.IsNullOrEmpty(changeVector))
                    continue;

                ApplyFlatChangeVector(changeVector.AsSpan(), buffer, MergeMode.Max);
            }

            return buffer.SerializeVector();
        }

        var orderBuffer = MergeVectorBuffer ??= new EquatableList<ChangeVectorEntry>();
        var versionBuffer = MergeVectorVersionBuffer ??= new EquatableList<ChangeVectorEntry>();
        orderBuffer.Clear();
        versionBuffer.Clear();

        foreach (var changeVector in changeVectors)
        {
            if (string.IsNullOrEmpty(changeVector))
                continue;

            var changeVectorSpan = changeVector.AsSpan();
            var separatorIndex = ChangeVectorParts.GetCompositeSeparatorIndex(changeVectorSpan);
            ApplyFlatChangeVector(ChangeVectorParts.GetPart(changeVectorSpan, separatorIndex, SparrowChangeVectorPart.Order), orderBuffer, MergeMode.Max);
            ApplyFlatChangeVector(ChangeVectorParts.GetPart(changeVectorSpan, separatorIndex, SparrowChangeVectorPart.Version), versionBuffer, MergeMode.Max);
        }

        return ChangeVectorParts.ToComposite(orderBuffer.SerializeVector(), versionBuffer.SerializeVector());
    }

    /// <summary>
    /// Computes the legacy lower frontier by taking the minimum etag for matching database ids. The first vector seeds the result;
    /// later vectors lower matching entries and return null only when no entry matches at all.
    /// </summary>
    /// <example>
    /// <code>
    /// Whole:   [A:10-dbA, B:20-dbB] + [A:8-dbA, B:25-dbB] => A:8-dbA, B:20-dbB
    /// Whole:   [A:10-dbA, B:20-dbB] + [A:8-dbA]           => A:8-dbA, B:20-dbB
    /// Whole:   [A:10-dbA, B:20-dbB] + [C:5-dbC]           => null
    /// Version: [S1:500-dbS|A:10-dbA] + [S2:700-dbT|A:8-dbA] => A:8-dbA
    /// </code>
    /// </example>
    public static string MergeDown(ReadOnlySpan<string> changeVectors, ChangeVectorPart changeVectorPart)
    {
        if (changeVectorPart != ChangeVectorPart.Whole)
            return MergePartDown(changeVectors, changeVectorPart);

        if (ChangeVectorParts.HasComposite(changeVectors) == false)
            return MergePartDown(changeVectors, ChangeVectorPart.Whole);

        var order = MergePartDown(changeVectors, ChangeVectorPart.Order);
        if (order == null)
            return null;

        var version = MergePartDown(changeVectors, ChangeVectorPart.Version);
        return version != null
            ? ChangeVectorParts.ToComposite(order, version)
            : null;

        static string MergePartDown(ReadOnlySpan<string> vectors, ChangeVectorPart part)
        {
            var buffer = MergeVectorBuffer ??= new EquatableList<ChangeVectorEntry>();
            buffer.Clear();

            if (vectors.Length == 0 || string.IsNullOrEmpty(vectors[0]))
                return null;

            var first = ChangeVectorParts.GetPart(vectors[0].AsSpan(), ToSparrowPart(part));
            if (first.Length == 0)
                return null;

            ApplyFlatChangeVector(first, buffer, MergeMode.Max);

            for (int i = 1; i < vectors.Length; i++)
            {
                if (string.IsNullOrEmpty(vectors[i]))
                    return null;

                var current = ChangeVectorParts.GetPart(vectors[i].AsSpan(), ToSparrowPart(part));
                if (current.Length == 0)
                    return null;

                if (ApplyFlatChangeVector(current, buffer, MergeMode.Min) == false)
                    return null;
            }

            return buffer.SerializeVector();
        }
    }

    /// <summary>
    /// Parses one flat change vector and applies it to the accumulator as either a max merge or a down/min merge.
    /// </summary>
    /// <returns>
    /// In Min mode, true means the incoming vector matched at least one accumulated database id; false means no match was found.<br/>
    /// In Max mode, true means the incoming vector was valid and non-empty.
    /// </returns>
    /// <example>
    /// <code>
    /// Max: entries [A:10-dbA] + incoming [A:12-dbA] => true, A:12-dbA
    /// Max: entries [] + incoming [B:20-dbB]         => true, B:20-dbB
    /// Min: entries [A:10-dbA] + incoming [A:8-dbA]  => true, A:8-dbA
    /// Min: entries [A:10-dbA] + incoming [B:8-dbB]  => false
    /// </code>
    /// </example>
    private static bool ApplyFlatChangeVector(ReadOnlySpan<char> changeVector, List<ChangeVectorEntry> entries, MergeMode mode)
    {
        if (changeVector.Length == 0)
            return mode == MergeMode.Max;

        if (mode == MergeMode.Max)
            ChangeVectorParser.AssertChangeVector(changeVector);

        var current = 0;
        var start = 0;
        var state = State.Tag;
        int tag = -1;
        bool matchedAnyEntry = false;

        while (current < changeVector.Length)
        {
            switch (state)
            {
                case State.Tag:
                    if (changeVector[current] == ':')
                    {
                        tag = ChangeVectorParser.ParseNodeTag(changeVector, start, current - 1);
                        state = State.Etag;
                        start = current + 1;
                    }
                    current++;
                    break;

                case State.Etag:
                    if (changeVector[current] == '-')
                    {
                        var etag = ChangeVectorParser.ParseEtag(changeVector, start, current - 1);

                        if (current + ChangeVectorParser.DbBase64IdSize > changeVector.Length)
                            ThrowInvalidEndOfString("DbId", changeVector);

                        var dbId = changeVector.Slice(current + 1, 22);
                        if (ApplyEntry(entries, tag, etag, dbId, mode) && mode == MergeMode.Min)
                            matchedAnyEntry = true;

                        start = current + ChangeVectorParser.DbBase64IdSize;
                        current = start;
                        state = State.Whitespace;
                    }
                    current++;
                    break;

                case State.Whitespace:
                    if (char.IsWhiteSpace(changeVector[current]) ||
                        changeVector[current] == ',')
                    {
                        start++;
                        current++;
                    }
                    else
                    {
                        start = current;
                        current++;
                        state = State.Tag;
                    }
                    break;

                default:
                    ThrowInvalidState(state, changeVector);
                    break;
            }
        }

        if (state == State.Whitespace)
            return mode == MergeMode.Max || matchedAnyEntry;

        ThrowInvalidEndOfString(state.ToString(), changeVector);
        return false;

        static bool ApplyEntry(List<ChangeVectorEntry> entries, int tag, long etag, ReadOnlySpan<char> dbId, MergeMode mode)
        {
            for (int i = 0; i < entries.Count; i++)
            {
                if (dbId.SequenceEqual(entries[i].DbId.AsSpan()) == false)
                    continue;

                if (ShouldReplace(entries[i].Etag, etag, mode))
                    entries[i] = CreateEntry(tag, etag, dbId);

                return true;
            }

            if (mode == MergeMode.Max)
                entries.Add(CreateEntry(tag, etag, dbId));

            return false;
        }

        static bool ShouldReplace(long existingEtag, long etag, MergeMode mode)
        {
            return mode == MergeMode.Max
                ? existingEtag < etag
                : existingEtag > etag;
        }

        static ChangeVectorEntry CreateEntry(int tag, long etag, ReadOnlySpan<char> dbId)
        {
            return new ChangeVectorEntry
            {
                NodeTag = tag,
                Etag = etag,
                DbId = dbId.ToString()
            };
        }
    }

    private static SparrowChangeVectorPart ToSparrowPart(ChangeVectorPart part)
    {
        return part switch
        {
            ChangeVectorPart.Whole => SparrowChangeVectorPart.Whole,
            ChangeVectorPart.Order => SparrowChangeVectorPart.Order,
            ChangeVectorPart.Version => SparrowChangeVectorPart.Version,
            _ => throw new ArgumentOutOfRangeException(nameof(part), part, null)
        };
    }

    [DoesNotReturn]
    private static void ThrowInvalidEndOfString(string state, ReadOnlySpan<char> cv)
    {
        throw new ArgumentException("Expected " + state + ", but got end of string in : " + cv.ToString());
    }

    [DoesNotReturn]
    private static void ThrowInvalidState(State state, ReadOnlySpan<char> cv)
    {
        throw new ArgumentOutOfRangeException(state + " in " + cv.ToString());
    }
}
