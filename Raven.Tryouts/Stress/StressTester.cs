using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Raven.Tests.Stress
{
    public class StressTester : LocalClientTest
    {
        const string FilePath5MB = "Stress\\Data\\data_5MB.txt";
        const string FilePath500KB = "Stress\\Data\\data_500KB.txt";
        const string FilePath100KB = "Stress\\Data\\data_100KB.txt";

        public void munin_stress_testing_ravendb_5mb_in_single_session_in_memory()
        {
            var text = File.ReadAllText(FilePath5MB);

            var documentStore = NewDocumentStore("munin",true, text.Length * 102);
            JObject dummy = null;

            using (var session = documentStore.OpenSession())
            {
                for (int i = 0; i < 100; i++)
                {
                    dummy = new JObject();
                    var property = new JProperty("Content", text);
                    dummy.Add(property);
                    dummy.Add("Id", JToken.FromObject(Guid.NewGuid()));
                    // Create
                    session.Store(dummy);
                }
                session.SaveChanges();
            }
            Assert.True(true);
        }

        public void munin_stress_testing_ravendb_5mb_in_single_session_in_filesystem()
        {
            var text = File.ReadAllText(FilePath5MB);

            var documentStore = NewDocumentStore("munin", false);
            JObject dummy = null;

            using (var session = documentStore.OpenSession())
            {
                for (int i = 0; i < 100; i++)
                {
                    dummy = new JObject();
                    var property = new JProperty("Content", text);
                    dummy.Add(property);
                    dummy.Add("Id", JToken.FromObject(Guid.NewGuid()));
                    // Create
                    session.Store(dummy);
                }
                session.SaveChanges();
            }
            Assert.True(true);
        }

        public void munin_stress_testing_ravendb_100kb_in_filesystem()
        {
            var text = File.ReadAllText(FilePath100KB);

            var documentStore = NewDocumentStore("munin", false);
            JObject dummy = null;

            for (int i = 0; i < 100; i++)
            {
                using (var session = documentStore.OpenSession())
                {
                    for (int j = 0; j < 100; j++)
                    {
                        var id = Guid.NewGuid().ToString();
                        dummy = new JObject();
                        var property = new JProperty("Content", text);
                        dummy.Add(property);
                        dummy.Add("Id", id);
                        // Create
                        session.Store(dummy);
                    }
                    session.SaveChanges();
                }
            }

            Assert.True(true);
        }


        public void munin_stress_testing_ravendb_500kb_in_filesystem()
        {

            var text = File.ReadAllText(FilePath500KB);

            var documentStore = NewDocumentStore("munin", false);
            JObject dummy = null;

            for (int j = 0; j < 100; j++)
            {
                using (var session = documentStore.OpenSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        dummy = new JObject();
                        var property = new JProperty("Content", text);
                        dummy.Add(property);
                        dummy.Add("Id", JToken.FromObject(Guid.NewGuid()));
                        // Create
                        session.Store(dummy);
                    }
                    session.SaveChanges();
                }
            }
            Assert.True(true);
        }

        public void esent_stress_testing_ravendb_500kb_in_filesystem()
        {
            var text = File.ReadAllText(FilePath500KB);

            var documentStore = NewDocumentStore("esent", false);

            JObject dummy = null;

            for (int j = 0; j < 100; j++)
            {
                using (var session = documentStore.OpenSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        dummy = new JObject();
                        var property = new JProperty("Content", text);
                        dummy.Add(property);
                        dummy.Add("Id", JToken.FromObject(Guid.NewGuid()));
                        // Create
                        session.Store(dummy);
                    }
                    session.SaveChanges();
                }
            }
            Assert.True(true);
        }

        public void esent_stress_testing_ravendb_500kb_in_filesystem_case2()
        {
            var text = File.ReadAllText(FilePath500KB);

            var documentStore = NewDocumentStore("esent", false);

            JObject dummy = null;

            for (int j = 0; j < 1000; j++)
            {
                using (var session = documentStore.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        dummy = new JObject();
                        var property = new JProperty("Content", text);
                        dummy.Add(property);
                        dummy.Add("Id", JToken.FromObject(Guid.NewGuid()));
                        // Create
                        session.Store(dummy);
                    }
                    session.SaveChanges();
                }
            }
            Assert.True(true);
        }

        public void esent_stress_testing_ravendb_100kb_in_filesystem()
        {
            var text = File.ReadAllText(FilePath100KB);

            var documentStore = NewDocumentStore("esent", false);

            JObject dummy = null;

            for (int j = 0; j < 100; j++)
            {
                using (var session = documentStore.OpenSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        dummy = new JObject();
                        var property = new JProperty("Content", text);
                        dummy.Add(property);
                        dummy.Add("Id", JToken.FromObject(Guid.NewGuid()));
                        // Create
                        session.Store(dummy);
                    }
                    session.SaveChanges();
                }
            }
            Assert.True(true);
        }

        public void esent_stress_testing_ravendb_100kb_in_filesystem_with_indexing()
        {
            var text = File.ReadAllText(FilePath100KB);

            var documentStore = NewDocumentStore("esent", false);

            JObject dummy = null;

            for (int j = 0; j < 100; j++)
            {
                using (var session = documentStore.OpenSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        dummy = new JObject();
                        var property = new JProperty("Content", text);
                        dummy.Add(property);
                        dummy.Add("Id", JToken.FromObject(Guid.NewGuid()));
                        // Create
                        session.Store(dummy);
                    }
                    session.SaveChanges();
                    // Force indexing
                    var stored = session.Query<JObject>().Customize(x => x.WaitForNonStaleResults()).ToArray();
                    Assert.NotNull(stored);
                    Assert.NotEmpty(stored);
                }

            }
            Assert.True(true);
        }

        public void esent_stress_testing_ravendb_100kb_in_filesystem_with_indexing_case2()
        {
            var text = File.ReadAllText(FilePath100KB);

            var documentStore = NewDocumentStore("esent", false);

            JObject dummy = null;

            for (int j = 0; j < 100; j++)
            {
                using (var session = documentStore.OpenSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        dummy = new JObject();
                        var property = new JProperty("Content", text);
                        dummy.Add(property);
                        dummy.Add("Id", JToken.FromObject(Guid.NewGuid()));
                        // Create
                        session.Store(dummy);
                    }
                    session.SaveChanges();
                }
                // Force indexing
                using (var session = documentStore.OpenSession())
                {
                    var stored = session.Query<JObject>().Customize(x => x.WaitForNonStaleResults()).ToArray();
                    Assert.NotNull(stored);
                    Assert.NotEmpty(stored);
                }
            }
            Assert.True(true);
        }

    }
}
