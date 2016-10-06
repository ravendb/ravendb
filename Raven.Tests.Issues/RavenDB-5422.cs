using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Lucene.Net.Analysis;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Database.Indexing;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_5422:RavenTest
    {
        [Fact]
        public void ShouldBeAbleToQueryLuceneTokens()
        {
            using (var store = NewDocumentStore())            
            {
                store.ExecuteIndex(new Users_ByName());
                using (var session = store.OpenSession())
                {
                    session.Store(new User() { Name = "OR" });                    
                    session.Store(new User() { Name = "AND" });
                    session.Store(new User() { Name = "NOT" });
                    session.SaveChanges();
                    WaitForIndexing(store);
                    session.Query<User, Users_ByName>().Search(user => user.Name, "OR").Single();
                    session.Query<User, Users_ByName>().Search(user => user.Name, "AND").Single();
                    session.Query<User, Users_ByName>().Search(user => user.Name, "NOT").Single();
                }
            }
        }

        public class Users_ByName : AbstractIndexCreationTask<User>
        {
            public Users_ByName()
            {
                Map = users => from user in users select new {user.Name};
                //Analyzers.Add(c => c.Name, typeof(LowerCaseKeywordAnalyzer).ToString());
            }
        }

        public class User
        {
            public string Name { get; set; }
        }
    }
}
