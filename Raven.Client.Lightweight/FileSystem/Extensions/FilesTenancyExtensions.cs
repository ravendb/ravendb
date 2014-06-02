using Raven.Abstractions.Data;
using Raven.Client.RavenFS;
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
        public static async Task EnsureFileSystemExistsAsync(this AsyncFilesServerClient client)
        {
            var existingSystems = await client.Admin.GetFileSystemsNames();

            if (existingSystems.Any(x => x.Equals(client.FileSystemName, StringComparison.InvariantCultureIgnoreCase)))
                return;

            await client.Admin.CreateFileSystemAsync(new DatabaseDocument
            {
                Id = "Raven/FileSystem/" + client.FileSystemName,
                Settings =
                 {
                     {"Raven/FileSystem/DataDir", Path.Combine("FileSystems", client.FileSystemName)}
                 }
            });
        }
    }
}
