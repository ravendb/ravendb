// -----------------------------------------------------------------------
//  <copyright file="RavenPerFieldAnalyzerWrapper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
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

        private class PerFieldAnalyzerComparer : IEqualityComparer<string>
        {
            public bool Equals(string inDictionary, string value)
            {
                if ( value[0] == '@' )
                {
                    for (int i = 1; i < inDictionary.Length - value.Length; i++ )
                    {
                        if ( inDictionary[i] == '<' )
                        {
                            i++;
                            for ( int v = 0; v < value.Length; v++, i++ )
                            {
                                if (value[v] != inDictionary[i])
                                    return false;
                            }

                            if (i == inDictionary.Length) // Not ending with '>'
                                return false;

                            return inDictionary[i] == '>';          
                        }
                    }
                    return false;
                }

                return string.Equals(inDictionary, value, System.StringComparison.Ordinal);
            }

            public int GetHashCode(string obj)
            {
                return obj.GetHashCode();
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