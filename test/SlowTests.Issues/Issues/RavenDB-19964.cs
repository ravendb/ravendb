using System;
using System.Collections.Generic;
using System.IO;
using FastTests;
using Raven.Client.Documents.Indexes;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19964 : RavenTestBase
{
    public RavenDB_19964(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Querying)]
    public void CheckIfDynamicReturnsAreMarkedForQuery()
    {
        using var store = GetDocumentStore();
        
        using var file1 = new MemoryStream();
        using var file2 = new MemoryStream();

        file1.Write(Encodings.Utf8.GetBytes(new string('a', 5)).AsSpan());
        file2.Write(Encodings.Utf8.GetBytes(new string('b', 4)).AsSpan());

        file1.Position = 0;
        file2.Position = 0;

        using (var session = store.OpenSession())
        {
            User u1 = new(){Name = "abc"};

            session.Store(u1);
            
            session.Advanced.Attachments.Store(u1.Id, "f1.txt", file1);
            session.Advanced.Attachments.Store(u1.Id, "f2.txt", file2);
            
            session.SaveChanges();
            
            store.ExecuteIndex(new UsersByAttachments());
            
            Indexes.WaitForIndexing(store);
            
            WaitForUserToContinueTheTest(store);

            string query = @"from index 'UsersByAttachments' where name = ""abc""";

            var res = session.Advanced
                .RawQuery<object>(query)
                .WaitForNonStaleResults().ToList();
            
            Assert.Equal(1, res.Count);
        }
    }
    
    private class User
    {
        public string Id { get; set; }
        
        public string Name { get; set; }
    }
    
    private class UsersByAttachments : AbstractJavaScriptIndexCreationTask
    {
        public UsersByAttachments()
        {
            Maps = new HashSet<string> { 
                @"map('Users', user => {
                      const attachments = attachmentsFor(user);
                      return attachments.map(a => {
                          return {
                              name: user.Name,
                              attachmentName: a.Name
                          };
                      });
                })" 
            };
        }
    }
}
