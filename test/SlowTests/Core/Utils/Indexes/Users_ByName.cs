// -----------------------------------------------------------------------
//  <copyright file="Users_ByName.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;

using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Client.Linq.Indexing;
using Raven.Tests.Core.Utils.Entities;

using User = SlowTests.Core.Utils.Entities.User;

namespace SlowTests.Core.Utils.Indexes
{
    public class Users_ByName : AbstractIndexCreationTask<User>
    {
        public Users_ByName()
        {
            Map = users => from u in users select new { Name = u.Name, LastName = u.LastName.Boost(10) };

            Indexes.Add(x => x.Name, FieldIndexing.Analyzed);

            IndexSuggestions.Add(x => x.Name);

            Analyzers.Add(x => x.Name, typeof(Lucene.Net.Analysis.SimpleAnalyzer).FullName);

            Stores.Add(x => x.Name, FieldStorage.Yes);
        }
    }
}
