// -----------------------------------------------------------------------
//  <copyright file="RavenPerFieldAnalyzerWrapper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;

using Lucene.Net.Analysis;
using Lucene.Net.Documents;

namespace Raven.Server.Documents.Indexes.Persistance.Lucene.Analyzers
{
    public sealed class RavenPerFieldAnalyzerWrapper : Analyzer
    {
        private readonly Analyzer _defaultAnalyzer;
        private readonly IDictionary<string, Analyzer> _analyzerMap = new Dictionary<string, Analyzer>(new PerFieldAnalyzerComparer());

        public class PerFieldAnalyzerComparer : IEqualityComparer<string>
        {
            public bool Equals(string inDictionary, string value)
            {
                if (value.Length == 0 || value[0] != '@')
                    return string.Equals(inDictionary, value, StringComparison.Ordinal);
                var start = value.IndexOf('<', 1) + 1;
                var end = value.IndexOf('>', start + 1);
                if (end == -1)
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
                    hash = obj[i] * 397 ^ hash;
                }
                return hash;
            }
        }

        public RavenPerFieldAnalyzerWrapper(Analyzer defaultAnalyzer)
        {
            _defaultAnalyzer = defaultAnalyzer;
        }

        public void AddAnalyzer(string fieldName, Analyzer analyzer)
        {
            _analyzerMap.Add(fieldName, analyzer);
        }

        public override TokenStream TokenStream(string fieldName, System.IO.TextReader reader)
        {
            return GetAnalyzer(fieldName).TokenStream(fieldName, reader);
        }

        internal Analyzer GetAnalyzer(string fieldName)
        {
            if (_analyzerMap.Count == 0)
                return _defaultAnalyzer;

            Analyzer value;
            _analyzerMap.TryGetValue(fieldName, out value);
            return value ?? _defaultAnalyzer;
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

        public override string ToString()
        {
            return "PerFieldAnalyzerWrapper(" + string.Join(",", _analyzerMap.Select(x => x.Key + " -> " + x.Value)) + ", default=" + _defaultAnalyzer + ")";
        }

        public override void Dispose()
        {
            _defaultAnalyzer?.Close();
            foreach (var analyzer in _analyzerMap.Values)
                analyzer?.Close();

            base.Dispose();
        }
    }
}