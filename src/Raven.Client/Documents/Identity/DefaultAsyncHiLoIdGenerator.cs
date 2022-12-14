using System;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Identity;

public class DefaultAsyncHiLoIdGenerator : AsyncHiLoIdGenerator
{
    public DefaultAsyncHiLoIdGenerator(string tag, DocumentStore store, string dbName, char identityPartsSeparator) : base(tag, store, dbName, identityPartsSeparator)
    {
    }

    protected virtual string GetDocumentIdFromId(NextId result)
    {
        return $"{Prefix}{result.Id}-{result.ServerTag}";
    }

    /// <summary>
    /// Generates the document ID.
    /// </summary>
    /// <param name="entity">The entity.</param>
    /// <returns></returns>
    public override async Task<string> GenerateDocumentIdAsync(object entity)
    {
        var result = await GetNextIdAsync().ConfigureAwait(false);
        _forTestingPurposes?.BeforeGeneratingDocumentId?.Invoke();
        return GetDocumentIdFromId(result);
    }

    internal TestingStuff _forTestingPurposes;

    internal TestingStuff ForTestingPurposesOnly()
    {
        if (_forTestingPurposes != null)
            return _forTestingPurposes;

        return _forTestingPurposes = new TestingStuff();
    }

    internal class TestingStuff
    {
        internal Action BeforeGeneratingDocumentId;
    }
}
