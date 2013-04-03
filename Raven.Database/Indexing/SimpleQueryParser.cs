//-----------------------------------------------------------------------
// <copyright file="SimpleQueryParser.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using Lucene.Net.Analysis;
using Lucene.Net.Search;
using Lucene.Net.Search.Function;
using Lucene.Net.Search.Spans;
using Raven.Abstractions.Data;
using Raven.Database.Indexing.LuceneIntegration;
using Raven.Database.Queries;

namespace Raven.Database.Indexing
{
	public class SimpleQueryParser
	{
		static readonly RavenPerFieldAnalyzerWrapper perFieldAnalyzerWrapper = new RavenPerFieldAnalyzerWrapper(new KeywordAnalyzer());

		public static HashSet<string> GetFields(IndexQuery query)
		{
			var hashSet = new HashSet<string>();
			if (string.IsNullOrWhiteSpace(query.Query))
				return hashSet;
			var q = QueryBuilder.BuildQuery(query.Query, query, perFieldAnalyzerWrapper);
			PopulateFields(q, hashSet);
			hashSet.Remove(string.Empty);
			return hashSet;
		}

		private static void PopulateFields(Query query, HashSet<string> fields)
		{
			if (query is MatchNoDocsQuery || 
				query is MatchAllDocsQuery || 
				query is ConstantScoreQuery || 
				query is CustomScoreQuery)
				return;

			var tq = query as TermQuery;
			if (tq != null)
			{
				fields.Add(tq.Term.Field);
				return;
			}
			var bq = query as BooleanQuery;
			if (bq != null)
			{
				foreach (var c in bq.Clauses)
				{
					PopulateFields(c.Query, fields);
				}
				return;
			}
			var sq = query as SpanQuery;
			if(sq != null)
			{
				fields.Add(sq.Field);
				return;
			}
			var rl = query as IRavenLuceneMethodQuery;
			if (rl != null)
			{
				fields.Add(rl.Field);
				return;
			}

			var dmq = query as DisjunctionMaxQuery;
			if(dmq != null)
			{
				foreach (var q in dmq)
				{
					PopulateFields(q, fields);
				}
				return;
			}
			var pq = query as PrefixQuery;
			if(pq != null)
			{
				fields.Add(pq.Prefix.Field);
				return;
			}
			var phq = query as PhraseQuery;
			if (phq != null)
			{
				foreach (var term in phq.GetTerms())
				{
					fields.Add(term.Field);
				}
				return;
			}
			var trq = query as TermRangeQuery;
			if(trq!=null)
			{
				fields.Add(trq.Field);
				return;
			}
			var wq = query as WildcardQuery;
			if(wq!= null)
			{
				fields.Add(wq.Term.Field);
				return;
			}
			var fq = query as FilteredQuery;
			if(fq != null)
			{
				PopulateFields(fq.Query, fields);
				return;
			}
			var mpq = query as MultiPhraseQuery;
			if(mpq != null)
			{
				foreach (var term in mpq.GetTermArrays().SelectMany(terms => terms))
				{
					fields.Add(term.Field);
				}
				return;
			}
			var fzq = query as FuzzyQuery;
			if(fzq != null)
			{
				fields.Add(fzq.Term.Field);
				return;
			}
			if (PopulateField<int>(query, fields)) 
				return;
			if (PopulateField<long>(query, fields))
				return;
			if (PopulateField<double>(query, fields))
				return;
			if (PopulateField<float>(query, fields))
				return;
			if (PopulateField<decimal>(query, fields))
				return;
			throw new InvalidOperationException("Don't know how to handle query of type: " + query.GetType().FullName + " " + query);
		}

		private static bool PopulateField<T>(Query query, HashSet<string> fields) where T : struct, IComparable<T>
		{
			var nfi = query as NumericRangeQuery<T>;
			if (nfi != null)
			{
				fields.Add(nfi.Field);
				return true;
			}
			return false;
		}

		public static HashSet<Tuple<string, string>> GetFieldsForDynamicQuery(IndexQuery query)
		{
			var results = new HashSet<Tuple<string, string>>();
			foreach (var result in GetFields(query))
			{
				if (result == "*")
					continue;
				results.Add(Tuple.Create(TranslateField(result), result));
			}
			return results;
		}

		public static string TranslateField(string field)
		{
			var fieldParts = field.Split(new[] { "." }, StringSplitOptions.RemoveEmptyEntries);

			var result = new StringBuilder();
			foreach (var fieldPart in fieldParts)
			{
				if ((char.IsLetter(fieldPart[0]) == false && fieldPart[0] != '_') ||
					fieldPart.Any(c => char.IsLetterOrDigit(c) == false && c != '_'
						&& c != ',' /* we allow the comma operator for collections */))
				{
					result.Append("[\"").Append(fieldPart).Append("\"]");
				}
				else
				{
					if (result.Length > 0)
						result.Append('.');

					result
						.Append(fieldPart);
				}
			}
			return result.ToString();
		}
	}
}
