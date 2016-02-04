using System;
using System.Threading.Tasks;

using Raven.Json.Linq;

namespace Raven.Smuggler.FileSystem
{
    public interface ISmuggleConfigurationsToDestination : IDisposable
    {
        Task WriteConfigurationAsync(string name, RavenJObject configuration);
    }
}