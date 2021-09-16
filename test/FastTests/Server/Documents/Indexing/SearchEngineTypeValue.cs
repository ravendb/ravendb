using System.Collections.Generic;
using Raven.Client.Documents.Indexes;

namespace FastTests.Server.Documents.Indexing
{
    public class SearchEngineTypeValue
    {
        public static IEnumerable<object[]> Data =>
            new List<object[]>
            {
                new object[] {SearchEngineType.Corax},
                new object[] {SearchEngineType.Lucene}
            };
    }
}
