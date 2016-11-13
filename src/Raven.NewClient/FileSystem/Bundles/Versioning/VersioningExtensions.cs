// -----------------------------------------------------------------------
//  <copyright file="VersioningExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using System.Threading.Tasks;

using Raven.Abstractions.FileSystem;

namespace Raven.NewClient.Client.FileSystem.Bundles.Versioning
{
    public static class VersioningExtensions
    {
        public static async Task<FileHeader[]> GetRevisionsForAsync(this IAsyncFilesSession session, string name, int start, int pageSize)
        {
            var inMemoryFilesSessionOperations = (InMemoryFilesSessionOperations)session;
            var revisions = await session.Commands.StartsWithAsync(name + "/revisions/", null, start, pageSize).ConfigureAwait(false);
            return revisions
                .Select(file =>
                {
                    inMemoryFilesSessionOperations.AddToCache(file.FullPath, file);
                    return file;
                })
                .ToArray();
        }

        public static async Task<string[]> GetRevisionNamesForAsync(this IAsyncFilesSession session, string name, int start, int pageSize)
        {
            var revisions = await session.Commands.StartsWithAsync(name + "/revisions/", null, start, pageSize).ConfigureAwait(false);
            return revisions
                .Select(x => x.FullPath)
                .ToArray();
        }
    }
}
