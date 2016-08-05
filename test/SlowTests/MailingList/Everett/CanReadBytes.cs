// -----------------------------------------------------------------------
//  <copyright file="CanReadBytes.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Document;
using Raven.Imports.Newtonsoft.Json;
using Xunit;

namespace SlowTests.MailingList.Everett
{
    public class CanReadBytes : RavenTestBase
    {
        [Fact]
        public async Task query_for_object_with_byte_array_with_TypeNameHandling_All()
        {
            using (var store = await GetDocumentStore())
            {
                store.Conventions = new DocumentConvention
                {
                    CustomizeJsonSerializer = serializer =>
                    {
                        serializer.TypeNameHandling = TypeNameHandling.All;
                    },
                };

                var json = GetResourceText("DocumentWithBytes.txt");
                var jsonSerializer = new DocumentConvention().CreateSerializer();
                var item = jsonSerializer.Deserialize<DesignResources>(new JsonTextReader(new StringReader(json)));

                using (var session = store.OpenSession())
                {
                    item.Id = "resources/123";
                    item.DesignId = "designs/123";
                    session.Store(item);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session
                        .Query<DesignResources>()
                        .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                        .Where(x => x.DesignId == "designs/123")
                        .ToList();
                }
            }
        }

        [Fact]
        public async Task query_for_object_with_byte_array_with_default_TypeNameHandling()
        {
            using (var store = await GetDocumentStore())
            {
                var json = GetResourceText("DocumentWithBytes.txt");
                var jsonSerializer = new DocumentConvention().CreateSerializer();
                var item = jsonSerializer.Deserialize<DesignResources>(new JsonTextReader(new StringReader(json)));

                using (var session = store.OpenSession())
                {
                    item.Id = "resources/123";
                    item.DesignId = "designs/123";
                    session.Store(item);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session
                        .Query<DesignResources>()
                        .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                        .Where(x => x.DesignId == "designs/123")
                        .ToList();
                }
            }
        }

        [Fact]
        public async Task load_object_with_byte_array_with_TypeNameHandling_All()
        {
            using (var store = await GetDocumentStore())
            {
                store.Conventions = new DocumentConvention
                {
                    CustomizeJsonSerializer = serializer =>
                    {
                        serializer.TypeNameHandling = TypeNameHandling.All;
                    },
                };

                var json = GetResourceText("DocumentWithBytes.txt");
                var jsonSerializer = new DocumentConvention().CreateSerializer();
                var item = jsonSerializer.Deserialize<DesignResources>(new JsonTextReader(new StringReader(json)));

                using (var session = store.OpenSession())
                {
                    item.Id = "resources/123";
                    item.DesignId = "designs/123";
                    session.Store(item);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                    session.Load<DesignResources>("resources/123");
            }
        }

        [Fact]
        public async Task load_object_with_byte_array_with_default_TypeNameHandling()
        {
            using (var store = await GetDocumentStore())
            {
                var json = GetResourceText("DocumentWithBytes.txt");
                var jsonSerializer = new DocumentConvention().CreateSerializer();
                var item = jsonSerializer.Deserialize<DesignResources>(new JsonTextReader(new StringReader(json)));

                using (var session = store.OpenSession())
                {
                    item.Id = "resources/123";
                    item.DesignId = "designs/123";
                    session.Store(item);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Load<DesignResources>("resources/123");
                }
            }
        }

        [Fact]
        public void FromText()
        {
            var json = GetResourceText("DocumentWithBytes.txt");
            var jsonSerializer = new DocumentConvention().CreateSerializer();
            var item = jsonSerializer.Deserialize<DesignResources>(new JsonTextReader(new StringReader(json)));
        }

        private static string GetResourceText(string name)
        {
            name = typeof(CanReadBytes).Namespace + "." + name;
            using (var stream = typeof(CanReadBytes).GetTypeInfo().Assembly.GetManifestResourceStream(name))
            {
                if (stream == null)
                    throw new InvalidOperationException("Could not find the following resource: " + name);
                return new StreamReader(stream).ReadToEnd();
            }
        }

        private class DesignResources
        {
            private List<Resource> _entries = new List<Resource>();

            public string DesignId { get; set; }

            public virtual string Id { get; set; }

            public DateTime LastSavedDate { get; set; }

            public string LastSavedUser { get; set; }

            public Guid SourceId { get; set; }

            public List<Resource> Entries
            {
                get { return _entries; }
                set
                {
                    if (value == null)
                        return;

                    _entries = value;
                }
            }
        }

        private class Resource
        {
            public Guid Id { get; set; }
            public byte[] Data { get; set; }
        }
    }
}
