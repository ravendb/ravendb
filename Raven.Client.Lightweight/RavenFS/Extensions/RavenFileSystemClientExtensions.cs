// -----------------------------------------------------------------------
//  <copyright file="RavenFileSystemClientExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net;

namespace Raven.Client.RavenFS.Extensions
{
    public static class RavenFileSystemClientExtensions
    {
         public static SynchronizationDestination ToSynchronizationDestination(this RavenFileSystemClient self)
         {
             var result = new SynchronizationDestination()
             {
                 FileSystem = self.FileSystemName,
                 ServerUrl = self.ServerUrl,
                 ApiKey = self.ApiKey
             };

             if (self.Credentials != null)
             {
                var networkCredential = self.Credentials as NetworkCredential;

                 if (networkCredential != null)
                 {
                     result.Username = networkCredential.UserName;
                     result.Password = networkCredential.Password;
                     result.Domain = networkCredential.Domain;
                 }
                 else
                 {
                     throw new InvalidOperationException("Expected NetworkCredential object while get: " + self.Credentials);
                 }
             }

             return result;
         }
    }
}