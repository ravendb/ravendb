using System;
using System.Collections;
using System.Collections.Generic;
using Org.BouncyCastle.Asn1.X509.Qualified;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public enum SearchEngineClassDataMode
    {
        OnlyCorax,
        OnlyLucene,
        AllEngines
    }
    public class SearchEngineClassDataAttribute : ClassDataAttribute
    {
        public SearchEngineClassDataAttribute() : base(typeof(SearchEngineMode.AllSearchEngines))
        {
        }

        private static Type GetType(SearchEngineType type)
            => type switch
            {
                SearchEngineType.Corax => typeof(SearchEngineMode.SearchEngineCorax),
                SearchEngineType.Lucene => typeof(SearchEngineMode.SearchEngineLucene),
                _ => typeof(SearchEngineMode.AllSearchEngines),
            };
        public SearchEngineClassDataAttribute(SearchEngineType useOnlyEngineType) : base(GetType(useOnlyEngineType))
        {
        }
    }

    public class SearchEngineMode
    {
        public class AllSearchEngines : SearchEngineTypeData
        {
            public AllSearchEngines()
            {
                _data = new()
                {
                    new object[] {SearchEngineType.Corax},
                    new object[] {SearchEngineType.Lucene}
                };
            }
        }
        
        public class SearchEngineCorax : SearchEngineTypeData
        {
            public SearchEngineCorax()
            {
                _data = new()
                {
                    new object[] {SearchEngineType.Corax}
                };
            }
        }
        
        public class SearchEngineLucene : SearchEngineTypeData
        {
            public SearchEngineLucene()
            {
                _data = new()
                {
                    new object[] {SearchEngineType.Lucene}
                };
            }
        }
        
        public abstract class SearchEngineTypeData : IEnumerable<object[]>
        {
            protected List<object[]> _data;
                

            public IEnumerator<object[]> GetEnumerator() => _data.GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }
    }
}
