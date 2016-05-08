using System.Linq;

using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Core.Utils.Entities;

using Company = SlowTests.Core.Utils.Entities.Company;
using Headquater = SlowTests.Core.Utils.Entities.Headquater;

namespace SlowTests.Core.Utils.Indexes
{
    public class MultiMapIndex : AbstractMultiMapIndexCreationTask<MultiMapIndex.Result>
    {
        public class Result
        {
            public object[] Content { get; set; }
        }

        public MultiMapIndex()
        {
            AddMap<Company>(items => from x in items
                                     select new Result { Content = new object[] { x.Address1, x.Address2, x.Address3 } });

            AddMap<Headquater>(items => from x in items
                                  select new Result { Content = new object[] { x.Address1, x.Address2, x.Address3 } });

            Index(x => x.Content, FieldIndexing.Analyzed);
        }
    }
}
