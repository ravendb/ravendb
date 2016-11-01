// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4161.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;
using Xunit.Sdk;

namespace FastTests.NewClient.ResultsTransformer
{
    public class RavenDB_4161 : RavenTestBase
    {
        [Fact(Skip = "Iftah - currently failing")]
        public void CanUseTransfromer()
        {
            using (var store = GetDocumentStore())
            {
                new Token_Id().Execute(store);
                using (var session = store.OpenNewSession())
                {
                    var token = new Token
                    {
                        Id = "Token/foo/" + Guid.NewGuid(),
                        Name = "Test1",
                        Data = "Test1"
                    };

                    var token2 = new Token
                    {
                        Id = "Token/foo/" + Guid.NewGuid(),
                        Name = "Test2",
                        Data = "Test2"
                    };

                    session.Store(token);
                    session.Store(token2);
                    session.SaveChanges();
                    WaitForIndexing(store);

                    //WaitForUserToContinueTheTest(store);

                    var fooTokens = session.Advanced.LoadStartingWith<Token_Id, string>("Token/foo").OrderBy(x => x).ToArray();

                    var fromQuery = session.Query<Token>().TransformWith<Token_Id, string>().ToArray().OrderBy(x => x).ToArray();

                    Assert.Equal(fooTokens, fromQuery, StringComparer.OrdinalIgnoreCase);

                    Assert.Equal(token2.Id, session.Load<Token_Id, string>(token2.Id));

                    Assert.Equal(token.Id, session.Advanced.Lazily.Load<Token_Id, string>(token.Id).Value);

                }
            }
        }
    
        [Fact(Skip = "Iftah - currently failing")]
        public async Task CanUseTransfromerAsync()
        {
            using (var store = GetDocumentStore())
            {
                await new Token_Id().ExecuteAsync(store);

                using (var session = store.OpenNewAsyncSession())
                {
                    var token = new Token
                    {
                        Id = "Token/foo/" + Guid.NewGuid(),
                        Name = "Test1",
                        Data = "Test1"
                    };

                    var token2 = new Token
                    {
                        Id = "Token/foo/" + Guid.NewGuid(),
                        Name = "Test2",
                        Data = "Test2"
                    };

                    await session.StoreAsync(token);
                    await session.StoreAsync(token2);
                    await session.SaveChangesAsync();
                    WaitForIndexing(store);

                }
                using (var session = store.OpenNewSession())
                {
                    //TODO iftah, change the session back to Async when Async session is fully implemented
                    var fooTokens = (session.Advanced.LoadStartingWith<Token_Id, string>("Token/foo")).OrderBy(x => x).ToArray();

                    var fromQuery = (session.Query<Token>().TransformWith<Token_Id, string>().ToList()).OrderBy(x => x).ToArray();

                    Assert.Equal(fooTokens, fromQuery, StringComparer.OrdinalIgnoreCase);

                }
            }
        }

        public class Token
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Data { get; set; }
        }

        public class Token_Id : AbstractTransformerCreationTask<Token>
        {
            public Token_Id()
            {
                TransformResults = tokens => from token in tokens select token.Id;
            }
        }
    }
}