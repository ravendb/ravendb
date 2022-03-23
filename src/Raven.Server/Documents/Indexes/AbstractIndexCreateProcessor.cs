using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.MapReduce.OutputToCollection;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Indexes;

namespace Raven.Server.Documents.Indexes;

public abstract class AbstractIndexCreateProcessor
{
    protected readonly ServerStore ServerStore;

    protected AbstractIndexCreateProcessor([NotNull] ServerStore serverStore)
    {
        ServerStore = serverStore ?? throw new ArgumentNullException(nameof(serverStore));
    }

    protected abstract string GetDatabaseName();

    protected abstract SystemTime GetDatabaseTime();

    protected abstract RavenConfiguration GetDatabaseConfiguration();

    protected abstract IndexContext GetIndex(string name);

    protected abstract IEnumerable<string> GetIndexNames();

    protected abstract ValueTask WaitForIndexNotificationAsync(long index);

    public void ValidateStaticIndex(IndexDefinition definition)
    {
        if (IndexStore.IsValidIndexName(definition.Name, true, out var errorMessage) == false)
        {
            throw new ArgumentException((errorMessage));
        }

        ServerStore.LicenseManager.AssertCanAddAdditionalAssembliesFromNuGet(definition);

        var safeFileSystemIndexName = IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(definition.Name);

        var indexWithFileSystemNameCollision = GetIndexNames().FirstOrDefault(x =>
            x.Equals(definition.Name, StringComparison.OrdinalIgnoreCase) == false &&
            safeFileSystemIndexName.Equals(IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(x), StringComparison.OrdinalIgnoreCase));

        if (indexWithFileSystemNameCollision != null)
            throw new IndexCreationException(
                $"Could not create index '{definition.Name}' because it would result in directory name collision with '{indexWithFileSystemNameCollision}' index");

        definition.RemoveDefaultValues();
        ValidateAnalyzers(definition);

        var databaseConfiguration = GetDatabaseConfiguration();
        var instance = IndexCompilationCache.GetIndexInstance(definition, databaseConfiguration, IndexDefinitionBaseServerSide.IndexVersion.CurrentVersion); // pre-compile it and validate

        if (definition.Type == IndexType.MapReduce)
        {
            // TODO [ppekrol]
            //MapReduceIndex.ValidateReduceResultsCollectionName(definition, instance, databaseConfiguration, NeedToCheckIfCollectionEmpty(definition, databaseConfiguration));

            if (string.IsNullOrEmpty(definition.PatternForOutputReduceToCollectionReferences) == false)
                OutputReferencesPattern.ValidatePattern(definition.PatternForOutputReduceToCollectionReferences, out _);
        }
    }

    public async ValueTask<long> CreateIndexInternalAsync(IndexDefinition definition, string raftRequestId, string source = null)
    {
        if (definition == null)
            throw new ArgumentNullException(nameof(definition));

        ValidateStaticIndex(definition);

        var databaseName = GetDatabaseName();
        var databaseTime = GetDatabaseTime();
        var databaseConfiguration = GetDatabaseConfiguration();

        var command = new PutIndexCommand(
            definition,
            databaseName,
            source,
            databaseTime.GetUtcNow(),
            raftRequestId,
            databaseConfiguration.Indexing.HistoryRevisionsNumber,
            databaseConfiguration.Indexing.StaticIndexDeploymentMode
        );

        long index = 0;
        try
        {
            index = (await ServerStore.SendToLeaderAsync(command)).Index;
        }
        catch (Exception e)
        {
            IndexStore.ThrowIndexCreationException("static", definition.Name, e, "the cluster is probably down", ServerStore);
        }

        try
        {
            await WaitForIndexNotificationAsync(index);
        }
        catch (TimeoutException toe)
        {
            IndexStore.ThrowIndexCreationException("static", definition.Name, toe, $"the operation timed out after: {ServerStore.Engine.OperationTimeout}.", ServerStore);
        }

        return index;
    }

    private void ValidateAnalyzers(IndexDefinition definition)
    {
        if (definition.Fields == null)
            return;

        foreach (var kvp in definition.Fields)
        {
            if (string.IsNullOrWhiteSpace(kvp.Value.Analyzer))
                continue;

            try
            {
                IndexingExtensions.GetAnalyzerType(kvp.Key, kvp.Value.Analyzer, GetDatabaseName());
            }
            catch (Exception e)
            {
                throw new IndexCompilationException(e.Message, e);
            }
        }
    }

    private bool NeedToCheckIfCollectionEmpty(IndexDefinition definition, RavenConfiguration databaseConfiguration)
    {
        var currentIndex = GetIndex(definition.Name);
        var replacementIndexName = Constants.Documents.Indexing.SideBySideIndexNamePrefix + definition.Name;
        var replacementIndex = GetIndex(replacementIndexName);
        if (currentIndex == null && replacementIndex == null)
        {
            // new index
            return true;
        }

        if (currentIndex == null)
        {
            // we deleted the in memory index but didn't delete the replacement yet
            return true;
        }

        var creationOptions = IndexStore.GetIndexCreationOptions(definition, currentIndex, databaseConfiguration, out var _);
        IndexCreationOptions replacementCreationOptions;
        if (replacementIndex != null)
        {
            replacementCreationOptions = IndexStore.GetIndexCreationOptions(definition, replacementIndex, databaseConfiguration, out var _);
        }
        else
        {
            // the replacement index doesn't exist
            return IsCreateOrUpdate(creationOptions);
        }

        return IsCreateOrUpdate(creationOptions) ||
               IsCreateOrUpdate(replacementCreationOptions);
    }

    private static bool IsCreateOrUpdate(IndexCreationOptions creationOptions)
    {
        return creationOptions == IndexCreationOptions.Create ||
               creationOptions == IndexCreationOptions.Update;
    }
}
