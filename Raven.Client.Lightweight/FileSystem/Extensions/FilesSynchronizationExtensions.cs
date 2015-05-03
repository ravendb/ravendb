using Raven.Abstractions.FileSystem;
using Raven.Client.FileSystem.Connection;
using System;
using System.Net;

namespace Raven.Client.FileSystem.Extensions
{
    public static class FilesSynchronizationExtensions
    {
        public static SynchronizationDestination ToSynchronizationDestination(this IAsyncFilesCommands self)
        {
            var selfImpl = (IAsyncFilesCommandsImpl)self;

            var result = new SynchronizationDestination()
            {
                FileSystem = self.FileSystem,
                ServerUrl = selfImpl.ServerUrl,               
            };

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
