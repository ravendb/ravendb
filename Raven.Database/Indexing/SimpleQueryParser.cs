//-----------------------------------------------------------------------
// <copyright file="SimpleQueryParser.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Linq;
using Raven.Abstractions.Data;

namespace Raven.Database.Indexing
{
	public class SimpleQueryParser
	{
		static readonly Regex queryTerms = new Regex(@"([^\s\(\+\-][\w._,]+?)\:", RegexOptions.Compiled);
		static readonly Regex dateQuery = new Regex(@"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{7}", RegexOptions.Compiled);
		static readonly Regex dynamicQueryTerms = new Regex(@"[-+]?([^\{\}\[\]\(\)\s]*?[^\\\s])\:", RegexOptions.Compiled);

		public static HashSet<string> GetFields(IndexQuery query)
		{
			return GetFieldsInternal(query, queryTerms);
		}

		public static HashSet<Tuple<string,string>> GetFieldsForDynamicQuery(IndexQuery query)
		{
			var results = new HashSet<Tuple<string,string>>();
			foreach (var result in GetFieldsInternal(query, dynamicQueryTerms))
			{
				if(result == "*")
					continue;
				results.Add(Tuple.Create(TranslateField(result), result));
			}
			
			return results;
		}
		private static HashSet<string> GetFieldsInternal(IndexQuery query, Regex queryTerms)
		{
			var fields = new HashSet<string>();
			if (string.IsNullOrEmpty(query.DefaultField) == false)
			{
				fields.Add(query.DefaultField);
			}
			if(query.Query == null)
				return fields;
			var dates = dateQuery.Matches(query.Query); // we need to exclude dates from this check
			var queryTermMatches = queryTerms.Matches(query.Query);
			for (int x = 0; x < queryTermMatches.Count; x++)
			{
				Match match = queryTermMatches[x];
				String field = match.Groups[1].Value;

				var isDate = false;
				for (int i = 0; i < dates.Count; i++)
				{
					if(match.Index < dates[i].Index)
						continue;
					if (match.Index >= dates[i].Index + dates[i].Length) 
						continue;

					isDate = true;
					break;
				}

				if (isDate == false)
				fields.Add(field);
			}
			return fields;
		}

		private static string TranslateField(string field)
		{
			var fieldParts = field.Split(new[]{"."}, StringSplitOptions.RemoveEmptyEntries);

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
