using System;
using JetBrains.Annotations;
using Raven.Server.Config.Categories;

namespace Raven.Server.Documents.Indexes;

public class IndexContext
{
    public IndexContext([NotNull] Index index)
    {
        if (index == null)
            throw new ArgumentNullException(nameof(index));

        Definition = index.Definition;
        Configuration = index.Configuration;
    }

    public readonly IndexDefinitionBaseServerSide Definition;

    public readonly IndexingConfiguration Configuration;
}

public static class IndexContextExtensions
{
    public static IndexContext ToIndexContext(this Index index) => new IndexContext(index);
}
