using System;
using System.Collections.Generic;
using System.Numerics;

namespace Tests.Infrastructure.XunitExtensions;

public static class RavenTraitHelper
{
    private const int MaxFlagCount = 64;
    private const string Category = "Category";
    private static readonly RavenTestCategory[] AllTestCategories = Enum.GetValues<RavenTestCategory>();
    private static readonly string[] CategoryNames = BuildCategoryNames();
    private static readonly IReadOnlyCollection<KeyValuePair<string, string>>[] SingleCategoryTraits = BuildSingleCategoryTraits();

    public static IReadOnlyCollection<KeyValuePair<string, string>> GetTraitsFor(RavenTestCategory category)
    {
        int distinctCategories = CountDistinctCategories(category);

        if (distinctCategories == 0)
        {
            return [];
        }
        
        if (distinctCategories == 1)
        {
            int index = GetIndexFor(category);
            return SingleCategoryTraits[index] ?? [new KeyValuePair<string, string>(Category, CategoryNames[index])];
        }

        var list = new KeyValuePair<string, string>[distinctCategories];
        var at = 0;

        // Walk only the bits that are actually set: take the lowest set bit, map it to its name,
        // clear it with an xor, and repeat until none remain. That's exactly distinctCategories
        // iterations (two or three in practice) instead of scanning every declared category.
        var bits = (ulong)category;
        while (bits != 0)
        {
            var index = BitOperations.TrailingZeroCount(bits);
            list[at++] = new KeyValuePair<string, string>(Category, CategoryNames[index]);
            bits ^= 1UL << index;
        }

        return list;
    }

    private static int CountDistinctCategories(RavenTestCategory category) => BitOperations.PopCount((ulong)category);

    private static int GetIndexFor(RavenTestCategory category) => BitOperations.Log2((ulong)category);

    /// <summary>
    /// Builds all category names.
    /// </summary>
    private static string[] BuildCategoryNames()
    {
        var names = new string[MaxFlagCount];
        foreach (RavenTestCategory value in AllTestCategories)
        {
            if (value == RavenTestCategory.None)
                continue;

            names[GetIndexFor(value)] = value.ToString();
        }
        return names;
    }

    /// <summary>
    /// Pre-builds the trait list for every single-bit category, indexed by the flag's bit position.
    /// Composite flags (more than one bit set, e.g. BulkInsert) are left null and fall back to the scan.
    /// </summary>
    private static IReadOnlyCollection<KeyValuePair<string, string>>[] BuildSingleCategoryTraits()
    {
        var traits = new IReadOnlyCollection<KeyValuePair<string, string>>[MaxFlagCount];
        foreach (RavenTestCategory value in AllTestCategories)
        {
            if (value == RavenTestCategory.None)
                continue;

            // Check if it's an actual flag
            if (BitOperations.PopCount((ulong)value) != 1)
                continue;

            var index = GetIndexFor(value);
            traits[index] = [new KeyValuePair<string, string>(Category, CategoryNames[index])];
        }
        return traits;
    }
}
