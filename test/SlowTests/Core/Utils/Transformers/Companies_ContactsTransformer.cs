using System.Linq;
using Raven.Client.Indexes;
using SlowTests.Core.Utils.Entities;

namespace SlowTests.Core.Utils.Transformers
{
    public class Companies_ContactsTransformer : AbstractTransformerCreationTask<Company>
    {
        public class Result
        {
            public string Email { get; set; }
        }

        public Companies_ContactsTransformer()
        {
            TransformResults = companies => from company in companies
                                            from contact in company.Contacts
                                            select new
                                            {
                                                contact.Email
                                            };
        }
    }
}