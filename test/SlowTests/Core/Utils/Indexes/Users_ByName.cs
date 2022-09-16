// -----------------------------------------------------------------------
//  <copyright file="Users_ByName.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using User = SlowTests.Core.Utils.Entities.User;

namespace SlowTests.Core.Utils.Indexes
{
    public class Users_ByName : AbstractIndexCreationTask<User>
    {
        public Users_ByName()
        {
            Map = users => from u in users select new { Name = u.Name, LastName = u.LastName.Boost(10) };

            Indexes.Add(x => x.Name, FieldIndexing.Search);

            IndexSuggestions.Add(x => x.Name);

            Analyzers.Add(x => x.Name, typeof(Lucene.Net.Analysis.SimpleAnalyzer).FullName);

            Stores.Add(x => x.Name, FieldStorage.Yes);
        }
    }

    public class Users_ByName_WithoutBoosting : AbstractIndexCreationTask<User>
    {
        public Users_ByName_WithoutBoosting()
        {
            Map = users => from u in users select new { Name = u.Name, LastName = u.LastName };

            Indexes.Add(x => x.Name, FieldIndexing.Search);

            IndexSuggestions.Add(x => x.Name);

            Analyzers.Add(x => x.Name, typeof(Lucene.Net.Analysis.SimpleAnalyzer).FullName);

            Stores.Add(x => x.Name, FieldStorage.Yes);
        }
    }
}
