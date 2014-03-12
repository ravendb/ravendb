// -----------------------------------------------------------------------
//  <copyright file="FileSystemTenancyExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;

namespace Raven.Client.RavenFS.Extensions
{
    public static class FileSystemTenancyExtensions
    {
         public static async Task EnsureFileSystemExistsAsync(this RavenFileSystemClient client)
         {
             var existingSystems = await client.Admin.GetFileSystemsNames();

             if (existingSystems.Any(x => x.Equals(client.FileSystemName, StringComparison.InvariantCultureIgnoreCase)))
                 return;

             await client.Admin.CreateFileSystemAsync(new DatabaseDocument
             {
                 Id = "Raven/FileSystem/" + client.FileSystemName,
                 Settings =
                 {
                     {"Raven/FileSystem/DataDir", Path.Combine("~", Path.Combine("FileSystems", client.FileSystemName))}
                 }
             });
         }
    }
}