using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Extensions;

namespace Raven.Client.FileSystem.Extensions
{
    public static class FilesTenancyExtensions
    {
        public static async Task EnsureFileSystemExistsAsync(this IAsyncFilesCommands commands)
        {
            var existingSystems = await commands.Admin.GetNamesAsync().ConfigureAwait(false);
            if (existingSystems.Any(x => x.Equals(commands.FileSystemName, StringComparison.InvariantCultureIgnoreCase)))
                return;

            await commands.Admin.CreateFileSystemAsync(MultiDatabase.CreateFileSystemDocument(commands.FileSystem)).ConfigureAwait(false);
                 }
        }
    }
