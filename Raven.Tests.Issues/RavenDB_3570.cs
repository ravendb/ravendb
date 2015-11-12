using System;
using System.Collections.Generic;
using System.DirectoryServices.AccountManagement;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Client.FileSystem;
using Raven.Database.Server.Security.Windows;
using Raven.Json.Linq;
using Raven.Server;
using Xunit;

namespace Raven.Tests.Issues
{

    public class RavenDB_3570
    {
        [Fact]
        public void CredentialsAreLoadedFromConnectionString()
        {
            using (var store = new FilesStore()
            {
                ConnectionStringName = "RavenFS"
            })
            {
                var credentials = (NetworkCredential)store.Credentials;

                Assert.Equal("local_user_test", credentials.UserName);
                Assert.Equal("local_user_test", credentials.Password);
                Assert.Equal(string.Empty, credentials.Domain);
            }
        }
    }
}
