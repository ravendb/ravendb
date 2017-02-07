// -----------------------------------------------------------------------
//  <copyright file="AccessingMetadataInTransformer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.NewClient.Client.Indexes;
using Xunit;

namespace SlowTests.Bugs
{
    public class AccessingMetadataInTransformer : RavenNewTestBase
    {
        [Fact(Skip = "AbstractIndexCreationTask.MetadataFor() is not supported")]
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
                    var load = session.Load<Profile>("profiles/1");

                    var meta = session.Advanced.GetMetadataFor(load);


                    var result = session.Query<Profile>()
                                   .Customize(c => c.WaitForNonStaleResults())
                                   .TransformWith<Transformer, Transformed>()
                                   .ToList();
                    
                    var transformed = result.First();
                    Assert.True(DateTime.UtcNow - transformed.DateUpdated < TimeSpan.FromSeconds(5), transformed.DateUpdated.ToString("O"));
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
                        DateUpdated = MetadataFor(doc).Value<DateTime>("Last-Modified")
                    };
            }
        }

        private class Profile
        {
            public String Name { get; set; }
        }
    }
}
