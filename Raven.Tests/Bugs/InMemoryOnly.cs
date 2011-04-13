//-----------------------------------------------------------------------
// <copyright file="InMemoryOnly.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.IO;
using Raven.Client.Client;
using Raven.Client.Embedded;
using Raven.Http;
using Xunit;

namespace Raven.Tests.Bugs
{
    public class InMemoryOnly
    {
        [Fact]
        public void InMemoryDoesNotCreateDataDir()
        {
            if(Directory.Exists("Data"))
                Directory.Delete("Data", true);

            NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8080);
            using (var store = new EmbeddableDocumentStore
            {
                RunInMemory = true,
                UseEmbeddedHttpServer = true,
                Configuration = 
                {
                    Port = 8080,
                    RunInMemory = true
                }
            })
            {
                store.Initialize();

                Assert.False(Directory.Exists("Data"));
            }
        }
    }
}