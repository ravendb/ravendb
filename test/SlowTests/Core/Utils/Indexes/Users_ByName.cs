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

#if !DNXCORE50
using Lucene.Net.Analysis;
#endif

namespace SlowTests.Core.Utils.Indexes
{
    public class Users_ByName : AbstractIndexCreationTask<User>
    {
        public Users_ByName()
        {
            Map = users => from u in users select new { Name = u.Name, LastName = u.LastName.Boost(10) };

            Indexes.Add(x => x.Name, FieldIndexing.Analyzed);

            IndexSuggestions.Add(x => x.Name);

#if !DNXCORE50
            Analyzers.Add(x => x.Name, typeof(SimpleAnalyzer).FullName);
#else
            Analyzers.Add(x => x.Name, "Lucene.Net.Analysis.SimpleAnalyzer");
#endif

            Stores.Add(x => x.Name, FieldStorage.Yes);
        }
    }
}
