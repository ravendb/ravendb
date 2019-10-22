// -----------------------------------------------------------------------
//  <copyright file="RavenDB_13975.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13975 : RavenTestBase
    {
        public RavenDB_13975(ITestOutputHelper output) : base(output)
        {
        }

        private class Entity
        {
            public string Chars { get; set; }
        }

        [Fact]
        public void CanGetDocumentsWithEscapeCharacters()
        {
            using (var store = GetDocumentStore())
            {
                var chars = new char[1024];
                for (var j = 0; j < chars.Length; j++)
                {
                    chars[j] = j % 2 == 0 ? (char)32 : (char)0;
                }

                var str = new string(chars);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 30; i++)
                    {
                        session.Store(new Entity
                        {
                            Chars = str
                        });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var entities = session.Query<Entity>().ToList();
                    Assert.Equal(30, entities.Count);
                }
            }
        }
    }
}
