using System;
using System.Net;

using Raven.Abstractions.FileSystem;
using Raven.NewClient.Client.FileSystem.Connection;

namespace Raven.NewClient.Client.FileSystem.Extensions
{
    public static class FilesSynchronizationExtensions
    {
        public static SynchronizationDestination ToSynchronizationDestination(this IAsyncFilesCommands self)
        {
            var selfImpl = (IAsyncFilesCommandsImpl)self;

            var result = new SynchronizationDestination
            {
                FileSystem = self.FileSystemName,
                ServerUrl = selfImpl.ServerUrl,               
            };

            if (selfImpl.Conventions != null)
            {
                result.AuthenticationScheme = self.Conventions.AuthenticationScheme;
            }

            if (self.PrimaryCredentials != null)
            {                
                var networkCredential = self.PrimaryCredentials.Credentials as NetworkCredential;

                if (networkCredential != null)
                {
                    result.Username = networkCredential.UserName;
                    result.Password = networkCredential.Password;
                    result.Domain = networkCredential.Domain;
                }
                else
                {
                    throw new InvalidOperationException("Expected NetworkCredential object while get: " + self.PrimaryCredentials.Credentials);
                }

                result.ApiKey = self.PrimaryCredentials.ApiKey;
            }

            return result;
        }
    }
}
