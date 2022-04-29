using System;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;

namespace Raven.Server.Documents.Indexes;

public abstract class AbstractIndexHasChangedController
{
    protected abstract IndexInformationHolder GetIndex(string name);

    protected abstract RavenConfiguration GetDatabaseConfiguration();

    public bool HasChanged(IndexDefinition definition)
    {
        if (definition == null)
            throw new ArgumentNullException(nameof(definition));

        if (IndexStore.IsValidIndexName(definition.Name, true, out var errorMessage) == false)
        {
            throw new ArgumentException(errorMessage);
        }

        var existingIndex = GetIndex(definition.Name);
        if (existingIndex == null)
            return true;

        var creationOptions = IndexStore.GetIndexCreationOptions(definition, existingIndex, GetDatabaseConfiguration(), out IndexDefinitionCompareDifferences _);
        return creationOptions != IndexCreationOptions.Noop;
    }
}
