// -----------------------------------------------------------------------
//  <copyright file="SmuggleConfigurationsToFileSystem.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;

using Raven.Abstractions.Util;
using Raven.Json.Linq;
using Raven.Smuggler.FileSystem;

namespace Raven.Database.FileSystem.Smuggler.Embedded
{
    internal class SmuggleConfigurationsToEmbedded : ISmuggleConfigurationsToDestination
    {
        private readonly RavenFileSystem fileSystem;

        public SmuggleConfigurationsToEmbedded(RavenFileSystem fileSystem)
        {
            this.fileSystem = fileSystem;
        }

        public void Dispose()
        {
        }

        public Task WriteConfigurationAsync(string name, RavenJObject configuration)
        {
            fileSystem.Storage.Batch(accessor => accessor.SetConfig(name, configuration));

            return new CompletedTask();
        }
    }
}