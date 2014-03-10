// -----------------------------------------------------------------------
//  <copyright file="RavenFileSystemClientExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Client.RavenFS.Extensions
{
    public static class RavenFileSystemClientExtensions
    {
         public static SynchronizationDestination ToSynchronizationDestination(this RavenFileSystemClient self)
         {
             return new SynchronizationDestination()
             {
                 FileSystem = self.FileSystemName,
                 ServerUrl = self.ServerUrl,
                 ApiKey = self.ApiKey
             };
         }
    }
}