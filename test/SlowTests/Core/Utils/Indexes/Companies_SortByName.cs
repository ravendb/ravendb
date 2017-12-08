// -----------------------------------------------------------------------
//  <copyright file="QueryResultsStreaming.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers.Collation.Cultures;
using Company = SlowTests.Core.Utils.Entities.Company;

namespace SlowTests.Core.Utils.Indexes
{
    public class Companies_SortByName : AbstractIndexCreationTask<Company>
    {

        public Companies_SortByName()
        {
            Map = companies => from company in companies
                               select new { company.Name };

            Analyzers.Add(c => c.Name, typeof(PlCollationAnalyzer).ToString());
        }
    }
}
