using System.Linq;
using Raven.Client.Documents.Indexes;
using Company = SlowTests.Core.Utils.Entities.Company;

namespace SlowTests.Core.Utils.Indexes
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
                                   Query = AsJson(company).Select(x => x.Value)
                               };

            Index(x => x.Query, FieldIndexing.Search);
        }
    }
}
