using Raven.Client.Documents.Session;

namespace Raven.Quill.Raven;

internal static class AsyncSessionExtensions
{
    internal static async Task<List<T>> LoadAllStartingWithAsync<T>(
        this IAsyncDocumentSession session, string prefix, CancellationToken ct)
    {
        const int pageSize = 1024;
        var items = new List<T>();
        for (var start = 0;; start += pageSize)
        {
            var page = (await session.Advanced.LoadStartingWithAsync<T>(
                prefix, start: start, pageSize: pageSize, token: ct)).ToArray();
            items.AddRange(page);
            if (page.Length < pageSize)
                break;
        }

        return items;
    }
}
