using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Database.Indexing.IndexMerging;
using Raven.Tests.Bundles.Authorization.Bugs;
using Raven.Tests.Bundles.Compression;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3086 :RavenTestBase
    {
         public class User
        {
            public String Name { get; set; }
            public String Email { get; set; }
            public int Age { get; set; }
        }
         public class UsersByName : AbstractIndexCreationTask<User>
        {
            public UsersByName()
            {
                Map = users => from user in users
                    select new {user.Name};
                Index(x => x.Name, FieldIndexing.Analyzed);
                
            }
        }
          public class UsersByEmail : AbstractIndexCreationTask<User>
        {
            public UsersByEmail()
            {
                Map = users => from user in users
                    select new {user.Email};
                
            }
        }
       /* public class Result 
        {
            public Dictionary<int, IndexDefinition> Indexs { get; set; }
            public IndexMergeResults Results { get; set; }
        }*/

        [Fact]
        public void IndexMergeWithField()
        {

            using (var store = NewDocumentStore())
            {
                new UsersByName().Execute(store);
                new UsersByEmail().Execute(store);

               var index1 = store.DatabaseCommands.GetIndex("UsersByName");
              
               var index2 = store.DatabaseCommands.GetIndex("UsersByEmail");
                
                
                var dictionary = new Dictionary<int, IndexDefinition>()
                {
                    {index1.IndexId,index1},
                    {index2.IndexId,index2}
                };
                IndexMerger merger = new IndexMerger(dictionary);
                IndexMergeResults results = merger.ProposeIndexMergeSuggestions();
                //Assert.Equal();
                if (results.Suggestions.Count != 0)
                {
                    foreach (var suggestion in results.Suggestions)
                    {
                        var ind = suggestion.MergedIndex;
                        Assert.Equal(ind.Fields.Count, index1.Fields.Union(index2.Fields).Distinct().Count());


                    }
                }

            }
            
        }


    }
}

