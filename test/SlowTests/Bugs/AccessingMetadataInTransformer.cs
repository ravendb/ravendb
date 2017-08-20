// -----------------------------------------------------------------------
//  <copyright file="AccessingMetadataInTransformer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.Client;
using Xunit;

namespace SlowTests.Bugs
{
    public class AccessingMetadataInTransformer : RavenTestBase
    {
        [Fact]
        public void ShouldNotResultInNullReferenceException()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Profile {Name = "1"});
                    session.Store(new Profile {Name = "2"});
                    session.SaveChanges();
                }

                store.ExecuteTransformer(new Transformer());

                using (var session = store.OpenSession())
                {
                    var load = session.Load<Profile>("profiles/1-A");

                    var meta = session.Advanced.GetMetadataFor(load);

                    var result = session.Query<Profile>()
                                   .Customize(c => c.WaitForNonStaleResults())
                                   .TransformWith<Transformer, Transformed>()
                                   .ToList();
                    
                    var transformed = result.First();
                    var now = DateTime.UtcNow;
                    var timeSpan = TimeSpan.FromSeconds(15);
                    Assert.True(now - transformed.DateUpdated < timeSpan, $"{now} - {transformed.DateUpdated:O} < {timeSpan} failed");
                }
            }
        }

        private class Transformed
        {
            public DateTime DateUpdated { get; set; }
        }

        private class Transformer : AbstractTransformerCreationTask<Profile>
        {
            public Transformer()
            {
                TransformResults = docs =>
                    from doc in docs
                    select new Transformed
                    {                       
                        DateUpdated = MetadataFor(doc).Value<DateTime>(Constants.Documents.Metadata.LastModified)
                    };
            }
        }

        private class Profile
        {
            public String Name { get; set; }
        }
    }
}
