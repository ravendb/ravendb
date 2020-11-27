using System;
using Raven.Client.Documents;

namespace Raven.Client.Extensions
{
    internal static class DocumentStoreExtensions
    {
        internal static string GetDatabase(this IDocumentStore store, string database)
        {
            database ??= store.Database;

            if (string.IsNullOrWhiteSpace(database) == false)
                return database;

            throw new InvalidOperationException($"Cannot determine database to operate on. Please either specify 'database' directly as an action parameter or set the default database to operate on using '{nameof(DocumentStore)}.{nameof(DocumentStore.Database)}' property. Did you forget to pass 'database' parameter?");
        }
    }
}
