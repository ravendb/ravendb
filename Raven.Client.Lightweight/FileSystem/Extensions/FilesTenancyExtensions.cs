using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem.Extensions
{
    public static class FilesTenancyExtensions
    {
        public static async Task EnsureFileSystemExistsAsync(this IAsyncFilesCommands commands)
        {
            var existingSystems = await commands.Admin.GetNamesAsync().ConfigureAwait(false);
            if (existingSystems.Any(x => x.Equals(commands.FileSystemName, StringComparison.OrdinalIgnoreCase)))
                return;

            await commands.Admin.CreateFileSystemAsync(new FileSystemDocument
            {
                Id = "Raven/FileSystem/" + commands.FileSystemName,
                Settings =
                 {
                     {Constants.FileSystem.DataDirectory, Path.Combine("FileSystems", commands.FileSystemName)}
                 }
            }).ConfigureAwait(false);
        }
    }
}
