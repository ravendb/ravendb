// -----------------------------------------------------------------------
//  <copyright file="IFileSystemSmugglerSource.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Database.Smuggler.FileSystem;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Util;
using Raven.Json.Linq;

namespace Raven.Smuggler.FileSystem
{
    public interface IFileSystemSmugglerSource : IDisposable
    {
        string DisplayName { get; }

        Task InitializeAsync(FileSystemSmugglerOptions options, CancellationToken cancellationToken);

        Task<IAsyncEnumerator<FileHeader>> GetFilesAsync(Etag lastEtag, int take, CancellationToken cancellationToken);

        Task<IEnumerable<KeyValuePair<string, RavenJObject>>> GetConfigurations(int skip, int take);

        Task<Stream> DownloadFileAsync(FileHeader file);

        Task<LastFilesEtagsInfo> FetchCurrentMaxEtagsAsync();

        IEnumerable<SmuggleType> GetItemsToSmuggle();
    }
}