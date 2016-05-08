// -----------------------------------------------------------------------
//  <copyright file="QueryResultsStreaming.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------
#if !DNXCORE50
using Lucene.Net;
#endif
using System.Linq;

using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Core.Utils.Entities;

using Company = SlowTests.Core.Utils.Entities.Company;

namespace SlowTests.Core.Utils.Indexes
{
    public class Companies_CustomAnalyzers : AbstractIndexCreationTask<Company>
    {
        public Companies_CustomAnalyzers()
        {
            Map = companies => from company in companies
                               select new
                               {
                                   company.Name,
                                   company.Desc,
                                   company.Email,
                                   company.Address1,
                                   company.Address2,
                                   company.Address3,
                                   company.Phone
                               };

#if !DNXCORE50
            Analyzers.Add(c => c.Name, typeof(Lucene.Net.Analysis.Standard.StandardAnalyzer).ToString());
            Analyzers.Add(c => c.Desc, typeof(Lucene.Net.Analysis.StopAnalyzer).ToString());
            Analyzers.Add(c => c.Email, typeof(Lucene.Net.Analysis.StopAnalyzer).ToString());
            Analyzers.Add(c => c.Address1, typeof(Lucene.Net.Analysis.SimpleAnalyzer).ToString());
            Analyzers.Add(c => c.Address2, typeof(Lucene.Net.Analysis.WhitespaceAnalyzer).ToString());
            Analyzers.Add(c => c.Address3, typeof(Lucene.Net.Analysis.KeywordAnalyzer).ToString());
            Analyzers.Add(c => c.Phone, typeof(Lucene.Net.Analysis.Standard.StandardAnalyzer).ToString());
#else
            Analyzers.Add(c => c.Name, "Lucene.Net.Analysis.Standard.StandardAnalyzer");
            Analyzers.Add(c => c.Desc, "Lucene.Net.Analysis.StopAnalyzer");
            Analyzers.Add(c => c.Email, "Lucene.Net.Analysis.StopAnalyzer");
            Analyzers.Add(c => c.Address1, "Lucene.Net.Analysis.SimpleAnalyzer");
            Analyzers.Add(c => c.Address2, "Lucene.Net.Analysis.WhitespaceAnalyzer");
            Analyzers.Add(c => c.Address3, "Lucene.Net.Analysis.KeywordAnalyzer");
            Analyzers.Add(c => c.Phone, "Lucene.Net.Analysis.Standard.StandardAnalyzer");
#endif

            Stores.Add(c => c.Name, FieldStorage.Yes);
            Stores.Add(c => c.Desc, FieldStorage.Yes);

            Indexes.Add(c => c.Email, FieldIndexing.NotAnalyzed);

            TermVectors.Add(c => c.Name, FieldTermVector.WithPositionsAndOffsets);
        }
    }
}
