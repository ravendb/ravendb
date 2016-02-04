// -----------------------------------------------------------------------
//  <copyright file="SmuggleConfigurationsToRemote.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;

using Raven.Client.FileSystem;
using Raven.Json.Linq;

namespace Raven.Smuggler.FileSystem.Remote
{
    public class SmuggleConfigurationsToRemote : ISmuggleConfigurationsToDestination
    {
        private readonly FilesStore store;

        public SmuggleConfigurationsToRemote(FilesStore store)
        {
            this.store = store;
        }

        public void Dispose()
        {
        }

        public Task WriteConfigurationAsync(string name, RavenJObject configuration)
        {
            return store.AsyncFilesCommands.Configuration.SetKeyAsync(name, configuration);
        }
    }
}