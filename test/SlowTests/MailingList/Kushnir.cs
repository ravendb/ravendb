// -----------------------------------------------------------------------
//  <copyright file="Kushnir.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class Kushnir : RavenTestBase
    {
        private const string Alphabet = "abcdefghijklmnopqrstuvwxyz";

        [Fact(Skip = "RavenDB-6264")]
        public void SortOnMetadata()
        {
            using (var store = GetDocumentStore())
            {
                //store.RegisterListener(new CreateDateMetadataConversion());
                new Foos_ByNameDateCreated().Execute(store);


                for (int i = 0; i < Alphabet.Length; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new Foo { Name = Alphabet[i].ToString() });
                        session.SaveChanges();
                    }
                }

                WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var ascending = session.Query<Foo, Foos_ByNameDateCreated>()
                        .Where(a => a.Name.StartsWith(string.Empty))
                        .OrderBy(a => a.DateCreated)
                        .Take(128)

                        .ToList();
                    var descending = session.Query<Foo, Foos_ByNameDateCreated>()
                        .Where(a => a.Name.StartsWith(string.Empty))
                        .OrderByDescending(a => a.DateCreated)
                        .Take(128)
                        .ToList();

                    Assert.Equal(Alphabet, ascending.Select(a => a.Name).Aggregate((a, b) => a + b));
                    Assert.Equal(new string(Alphabet.Reverse().ToArray()), @descending.Select(a => a.Name).Aggregate((a, b) => a + b));
                }
            }
        }

        private interface ITimeStamped { DateTime DateCreated { get; set; } }

        private class Foo : ITimeStamped
        {
            public string Id { get; set; }
            public string Name { get; set; }

            public DateTime DateCreated { get; set; }
        }

        /*
        private class CreateDateMetadataConversion : IDocumentConversionListener
        {
            private int _count;

            public void BeforeConversionToDocument(string key, object entity, RavenJObject metadata)
            {

            }

            public void AfterConversionToDocument(string key, object entity, RavenJObject document, RavenJObject metadata)
            {
                if (entity is ITimeStamped)
                {
                    document.Remove("DateCreated");
                    if (metadata["DateCreated"] == null)
                    {
                        metadata["DateCreated"] = DateTime.Today.AddDays(_count++).ToString("o");
                    }
                }
            }

            public void BeforeConversionToEntity(string key, RavenJObject document, RavenJObject metadata)
            {
            }

            public void AfterConversionToEntity(string key, RavenJObject document, RavenJObject metadata, object entity)
            {
                var timestamped = entity as ITimeStamped;
                if (timestamped != null && metadata.ContainsKey("DateCreated"))
                {
                    DateTime createDate = DateTime.Parse(metadata["DateCreated"].ToString());
                    DateTime.SpecifyKind(createDate, DateTimeKind.Utc);
                    timestamped.DateCreated = createDate;
                }
            }
        }
        */

        private class Foos_ByNameDateCreated : AbstractIndexCreationTask<Foo>
        {
            public Foos_ByNameDateCreated()
            {
                Map = foos => foos.Select(a => new { a.Name, DateCreated = MetadataFor(a).Value<DateTime>("DateCreated") });
            }
        }
    }
}
