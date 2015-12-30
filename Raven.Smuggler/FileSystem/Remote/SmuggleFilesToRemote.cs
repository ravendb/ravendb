// -----------------------------------------------------------------------
//  <copyright file="SmuggleFilesToRemote.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;

using Raven.Abstractions.FileSystem;
using Raven.Client.FileSystem;

namespace Raven.Smuggler.FileSystem.Remote
{
    internal class SmuggleFilesToRemote : ISmuggleFilesToDestination
    {
        private readonly FilesStore store;

        public SmuggleFilesToRemote(FilesStore store)
        {
            this.store = store;
        }

        public void Dispose()
        {
        }

        public Task WriteFileAsync(FileHeader file, Stream content)
        {
            return store.AsyncFilesCommands.UploadRawAsync(file.FullPath, content, file.Metadata, file.TotalSize.Value); //TODO arek - meybe use content.lenght if possible
        }
    }
}