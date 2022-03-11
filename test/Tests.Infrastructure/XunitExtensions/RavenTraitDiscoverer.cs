using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Utils;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Tests.Infrastructure.XunitExtensions;

public class RavenTraitDiscoverer : ITraitDiscoverer
{
    private static Array AllTestCategories;

    public RavenTraitDiscoverer()
    {
        AllTestCategories = Enum.GetValues(typeof(RavenTestCategory));
    }

    /// <inheritdoc />
    public virtual IEnumerable<KeyValuePair<string, string>> GetTraits(IAttributeInfo traitAttribute)
    {
        var list = traitAttribute.GetConstructorArguments()
            .Cast<RavenTestCategory>()
            .ToList();

        foreach (var category in GetFlags(list[0]))
            yield return new KeyValuePair<string, string>("Category", category.GetDescription());
    }

    private static IEnumerable<RavenTestCategory> GetFlags(RavenTestCategory category)
    {
        foreach (RavenTestCategory value in AllTestCategories)
        {
            if (value == RavenTestCategory.None)
                continue;

            if (category.HasFlag(value))
                yield return value;
        }
    }
}
