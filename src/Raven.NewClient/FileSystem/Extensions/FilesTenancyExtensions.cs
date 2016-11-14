using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.FileSystem;
using Raven.NewClient.Client.Extensions;

namespace Raven.NewClient.Client.FileSystem.Extensions
{
    public static class FilesTenancyExtensions
    {
        public static async Task EnsureFileSystemExistsAsync(this IAsyncFilesCommands commands)
        {
            var existingSystems = await commands.Admin.GetNamesAsync().ConfigureAwait(false);
            if (existingSystems.Any(x => x.Equals(commands.FileSystemName, StringComparison.OrdinalIgnoreCase)))
                return;
            
            var fileSystemDocument = MultiDatabase.CreateFileSystemDocument(commands.FileSystemName);
            
            await commands.Admin.CreateFileSystemAsync(fileSystemDocument).ConfigureAwait(false);
            
        }
    }
}
