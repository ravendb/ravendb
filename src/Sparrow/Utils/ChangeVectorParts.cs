using System;

namespace Sparrow.Utils;

internal enum ChangeVectorPart
{
    Whole,
    Order,
    Version
}

internal static class ChangeVectorParts
{
    /// <summary>
    /// Builds the canonical composite change vector string without spaces around the separator.
    /// </summary>
    /// <example>
    /// <code>
    /// order:   S1:500-dbA
    /// version: HA:100-dbB
    /// result:  S1:500-dbA|HA:100-dbB
    /// </code>
    /// </example>
    public static string ToComposite(string order, string version) => $"{order}|{version}";

    /// <summary>
    /// Returns true when at least one change vector in the list already uses the order|version shape.
    /// </summary>
    /// <example>
    /// <code>
    /// [A:10-dbA] + [S1:500-dbS|A:10-dbA] => true
    /// [A:10-dbA] + [B:20-dbB]            => false
    /// </code>
    /// </example>
    public static bool HasComposite(ReadOnlySpan<string> changeVectors)
    {
        foreach (var changeVector in changeVectors)
        {
            if (GetCompositeSeparatorIndex(changeVector.AsSpan()) >= 0)
                return true;
        }

        return false;
    }

    /// <summary>
    /// Selects the requested logical part from a change vector, treating mono vectors as both order and version.
    /// </summary>
    /// <example>
    /// <code>
    /// S1:500-dbS|A:10-dbA, Order   => S1:500-dbS
    /// S1:500-dbS|A:10-dbA, Version => A:10-dbA
    /// A:10-dbA, Version            => A:10-dbA
    /// A:10-dbA, Order              => A:10-dbA
    /// </code>
    /// </example>
    public static ReadOnlySpan<char> GetPart(ReadOnlySpan<char> changeVector, ChangeVectorPart part) => GetPart(changeVector, GetCompositeSeparatorIndex(changeVector), part);

    /// <summary>
    /// Selects the requested logical part using a known separator index to avoid searching for the pipe twice.
    /// </summary>
    /// <example>
    /// <code>
    /// S1:500-dbS|A:10-dbA, separator at '|', Order   => S1:500-dbS
    /// S1:500-dbS|A:10-dbA, separator at '|', Version => A:10-dbA
    /// </code>
    /// </example>
    public static ReadOnlySpan<char> GetPart(ReadOnlySpan<char> changeVector, int separatorIndex, ChangeVectorPart part)
    {
        if (changeVector.Length == 0)
            return ReadOnlySpan<char>.Empty;

        if (part == ChangeVectorPart.Whole || separatorIndex < 0)
            return changeVector;

        return part == ChangeVectorPart.Order
            ? changeVector.Slice(0, separatorIndex)
            : changeVector.Slice(separatorIndex + 1);
    }

    /// <summary>
    /// Returns the version part of a composite change vector, or the original mono vector unchanged.
    /// </summary>
    /// <example>
    /// <code>
    /// S1:500-dbS|A:10-dbA => A:10-dbA
    /// A:10-dbA            => A:10-dbA
    /// </code>
    /// </example>
    public static string GetVersion(string changeVector)
    {
        var separatorIndex = GetCompositeSeparatorIndex(changeVector.AsSpan());
        return separatorIndex < 0
            ? changeVector
            : changeVector.Substring(separatorIndex + 1);
    }

    /// <summary>
    /// Finds the single composite separator, returning -1 for a mono change vector and rejecting multiple separators.
    /// </summary>
    /// <example>
    /// <code>
    /// A:10-dbA                 => -1
    /// S1:500-dbS|A:10-dbA      => index of '|'
    /// S1:500-dbS|A:10|extra    => throws
    /// </code>
    /// </example>
    public static int GetCompositeSeparatorIndex(ReadOnlySpan<char> changeVector)
    {
        if (changeVector.Length == 0)
            return -1;

        var separatorIndex = changeVector.IndexOf('|');
        if (separatorIndex < 0)
            return -1;

        return changeVector.Slice(separatorIndex + 1).IndexOf('|') < 0
            ? separatorIndex
            : throw new ArgumentException($"Invalid change vector {changeVector.ToString()}");
    }
}
