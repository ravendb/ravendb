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
		private readonly IDictionary<string, Analyzer> analyzerMap = new Dictionary<string, Analyzer>();

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
			if (fieldName.StartsWith("@"))
			{
				var indexOfFieldStart = fieldName.IndexOf('<');
				var indexOfFieldEnd = fieldName.LastIndexOf('>');
				if (indexOfFieldStart != -1 && indexOfFieldEnd != -1)
				{
					fieldName = fieldName.Substring(indexOfFieldStart + 1, indexOfFieldEnd - indexOfFieldStart - 1);
				}
			}
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