using System;
using System.Collections.Generic;
using System.Reflection;
using Raven.Client.Documents.Indexes;
using Xunit.Sdk;

namespace Tests.Infrastructure
{
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
    public sealed class SearchEngineInlineDataAttribute : DataAttribute
    {
        private readonly object[][] data;

        public SearchEngineInlineDataAttribute(SearchEngineType type, params object[] data)
        {
            var newData = new object[data.Length + 1];

            newData[0] = type;
            for (int i = 0; i < data.Length; i++)
                newData[i + 1] = data[i];


            this.data = new[] { newData };
        }

        public SearchEngineInlineDataAttribute(params object[] data)
        {
            var newDataCorax = new object[data.Length + 1];
            var newDataLucene = new object[data.Length + 1];

            newDataCorax[0] = SearchEngineType.Corax;
            newDataLucene[0] = SearchEngineType.Lucene;

            for (int i = 0; i < data.Length; i++)
            {
                newDataCorax[i + 1] = data[i];
                newDataLucene[i + 1] = data[i];
            }

            this.data = new[] { newDataCorax, newDataLucene };
        }

        public override IEnumerable<object[]> GetData(MethodInfo testMethod)
        {
            return data;
        }
    }
}
