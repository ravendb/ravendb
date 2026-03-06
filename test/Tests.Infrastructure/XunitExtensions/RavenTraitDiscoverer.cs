using System;
using System.Collections.Generic;
using Raven.Server.Utils;

namespace Tests.Infrastructure.XunitExtensions;

public static class RavenTraitHelper
{
    private static readonly Array AllTestCategories = Enum.GetValues(typeof(RavenTestCategory));

    public static IReadOnlyCollection<KeyValuePair<string, string>> GetTraitsFor(RavenTestCategory category)
    {
        var list = new List<KeyValuePair<string, string>>();
        foreach (RavenTestCategory value in AllTestCategories)
        {
            if (value == RavenTestCategory.None)
                continue;

            if (category.HasFlag(value))
                list.Add(new KeyValuePair<string, string>("Category", value.GetDescription()));
        }
        return list;
    }
}
