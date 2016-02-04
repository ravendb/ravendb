// -----------------------------------------------------------------------
//  <copyright file="SmuggleFilesToFileSystem.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;

using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Util;
using Raven.Database.FileSystem.Actions;
using Raven.Smuggler.FileSystem;

namespace Raven.Database.FileSystem.Smuggler.Embedded
{
    internal class SmuggleFilesToEmbedded : ISmuggleFilesToDestination
    {
        private readonly RavenFileSystem fileSystem;

        public SmuggleFilesToEmbedded(RavenFileSystem fileSystem)
        {
            this.fileSystem = fileSystem;
        }

        public void Dispose()
        {
        }

        public Task WriteFileAsync(FileHeader file, Stream content)
        {
            return fileSystem.Files.PutAsync(file.FullPath, null, file.Metadata, () => new CompletedTask<Stream>(content), new FileActions.PutOperationOptions
            {
                ContentLength = file.TotalSize // TODO arek - check that
            });
        }
    }
}