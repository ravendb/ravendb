using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Tests.Common;
using System.Runtime.InteropServices;
using System.IO;
using System.IO.Compression;
using System.Net;
using Mono.CSharp;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Storage
{
    public class DocRefsDump : RavenTest
    {
        [Theory]
        [PropertyData("Storages")]
        public void DocRefsDumpShouldWork(string storage)
        {
            using (var store = NewRemoteDocumentStore(requestedStorage: storage, databaseName:"DocRefs"))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Pet(){Name="Chupa"},"pets/1");
                    session.Store(new Pet() { Name = "Blacky" }, "pets/2");
                    session.Store(new Pet() { Name = "Mitzy" }, "pets/3");
                    session.Store(new Person() { Name = "Tal", PetsIds = new List<string>(){ "pets/1","pets/2", "pets/3" } });
                    session.SaveChanges();
                }
                store.ExecuteIndex(new PeopleWithPetsIndex());
                WaitForIndexing(store);
                var actual = GetDocRefsOutput();
                Assert.Equal(expected, actual);

            }
        }

        private string GetDocRefsOutput()
        {
            var req = WebRequest.Create("http://localhost:8079/databases/DocRefs/debug/slow-dump-ref-csv");
            var webResponse = req.GetResponse();
            using (var streamReader = new StreamReader(webResponse.GetResponseStream()))
            {
                return streamReader.ReadToEnd();
            }            
        }
        
        public class PeopleWithPetsIndex: AbstractIndexCreationTask<Person> 
        {
            public PeopleWithPetsIndex()
            {
                Map = docs => from doc in docs
                    select new
                    {
                        Name = doc.Name,
                        Pets = doc.PetsIds.Select(LoadDocument<Pet>)
                    };
            }
        }

        private static string expected = "ref count,document key,sample references\r\n3,people/1,\"pets/1, pets/2, pets/3\"\r\n";
        public class Person
        {
            public string Name { get; set; }
            public List<string> PetsIds { get; set; } 
        }

        public class Pet
        {
            public string Name { get; set; }
        }
    }
}
