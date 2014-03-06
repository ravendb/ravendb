// -----------------------------------------------------------------------
//  <copyright file="FileSystemTenancyExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;

namespace Raven.Client.RavenFS.Extensions
{
    public static class FileSystemTenancyExtensions
    {
         public static Task EnsureFileSystemExistsAsync(this RavenFileSystemClient client)
         {
             // TODO arek - need to check if fs already exists, if yes then do nothing
             //if (fs != null)
             //    return;

             return client.Admin.CreateFileSystemAsync(new DatabaseDocument
             {
                 Id = "Raven/FileSystem/" + client.FileSystemName,
                 Settings =
                 {
                     {"Raven/FileSystem/DataDir", Path.Combine("~", Path.Combine("Databases", client.FileSystemName))}
                 }
             });
         }
    }
}