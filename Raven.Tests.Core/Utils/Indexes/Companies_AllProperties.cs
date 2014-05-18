using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Core.Utils.Entities;
using System.Linq;

namespace Raven.Tests.Core.Utils.Indexes
{
    public class Companies_AllProperties : AbstractIndexCreationTask<Company, Companies_AllProperties.Result>
    {
        public class Result
        {
            public string Query { get; set; }
        }

        public Companies_AllProperties()
        {
            Map = companies => from company in companies
                               select new
                               {
                                   Query = AsDocument(company).Select(x => x.Value)
                               };

            Index(x => x.Query, FieldIndexing.Analyzed);
        }
    }
}
