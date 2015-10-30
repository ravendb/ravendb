// -----------------------------------------------------------------------
//  <copyright file="RavenPerFieldAnalyzerWrapper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using System.Linq;

namespace Raven.Database.Indexing
{
    public sealed class RavenPerFieldAnalyzerWrapper : Analyzer
    {
        private readonly Analyzer defaultAnalyzer;
        private readonly IDictionary<string, Analyzer> analyzerMap = new Dictionary<string, Analyzer>( new PerFieldAnalyzerComparer() );

        public class PerFieldAnalyzerComparer : IEqualityComparer<string>
        {
            public bool Equals(string inDictionary, string value)
            {
                if (value.Length == 0 || value[0] != '@') 
                    return string.Equals(inDictionary, value, StringComparison.Ordinal);
                var start = value.IndexOf('<', 1) + 1;
                var end = value.IndexOf('>', start + 1);
                if(end == -1)
                    return string.Equals(inDictionary, value, StringComparison.Ordinal);
                return string.CompareOrdinal(inDictionary, 0, value, start, inDictionary.Length) == 0;
            }

            public int GetHashCode(string obj)
            {
                if (obj.Length == 0)
                    return -1;

                int start = 0;
                int end = obj.Length;
                if (obj[0] == '@')
                {
                    start = obj.IndexOf('<', 1) + 1;
                    end = obj.IndexOf('>', start + 1);
                    if (end == -1)
                        end = obj.Length;
                }

                var hash = 0;
                for (int i = start; i < end; i++)
                {
                    hash = obj[i]*397 ^ hash;
                }
                return hash;
            }
        }

        public RavenPerFieldAnalyzerWrapper(Analyzer defaultAnalyzer)
        {
            this.defaultAnalyzer = defaultAnalyzer;
        }

        public void AddAnalyzer(System.String fieldName, Analyzer analyzer)
        {
            analyzerMap[fieldName] = analyzer;
        }

        public override TokenStream TokenStream(System.String fieldName, System.IO.TextReader reader)
        {
            return GetAnalyzer(fieldName).TokenStream(fieldName, reader);
        }

        private Analyzer GetAnalyzer(string fieldName)
        {
            if (analyzerMap.Count == 0)
                return defaultAnalyzer;

            Analyzer value;
            analyzerMap.TryGetValue(fieldName, out value);
            return value ?? defaultAnalyzer;
        }

        public override TokenStream ReusableTokenStream(string fieldName, System.IO.TextReader reader)
        {
            return GetAnalyzer(fieldName).ReusableTokenStream(fieldName, reader);
        }

        public override int GetPositionIncrementGap(string fieldName)
        {
            return GetAnalyzer(fieldName).GetPositionIncrementGap(fieldName);
        }

        public override int GetOffsetGap(IFieldable field)
        {
            return GetAnalyzer(field.Name).GetOffsetGap(field);
        }

        public override System.String ToString()
        {
            return "PerFieldAnalyzerWrapper(" + string.Join(",", analyzerMap.Select(x => x.Key + " -> " + x.Value)) + ", default=" + defaultAnalyzer + ")";
        }
    }
}
