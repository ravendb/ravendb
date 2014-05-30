using Raven.Abstractions.RavenFS;
using Raven.Client.RavenFS;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem.Extensions
{
    public static class FilesSynchronizationExtensions
    {
        public static SynchronizationDestination ToSynchronizationDestination(this RavenFileSystemClient self)
        {
            var result = new SynchronizationDestination()
            {
                FileSystem = self.FileSystemName,
                ServerUrl = self.ServerUrl,
                ApiKey = self.ApiKey
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
            }

            return result;
        }
    }
}
