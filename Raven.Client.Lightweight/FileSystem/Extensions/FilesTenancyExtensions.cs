using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem.Extensions
{
    public static class FilesTenancyExtensions
    {
        public static async Task EnsureFileSystemExistsAsync(this IAsyncFilesCommands commands)
        {
            var existingSystems = await commands.Admin.GetNamesAsync().ConfigureAwait(false);
            if (existingSystems.Any(x => x.Equals(commands.FileSystem, StringComparison.InvariantCultureIgnoreCase)))
                return;

            await commands.Admin.CreateFileSystemAsync(new FileSystemDocument
            {
                Id = "Raven/FileSystem/" + commands.FileSystem,
                Settings =
                 {
                     {Constants.FileSystem.DataDirectory, Path.Combine("FileSystems", commands.FileSystem)}
                 }
            }).ConfigureAwait(false);
        }
    }
}
