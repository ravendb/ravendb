// -----------------------------------------------------------------------
//  <copyright file="CachedIndexedTerms.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Lucene.Net.Index;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Database.Indexing
{
	public class IndexedTerms
	{
		public static RavenJObject[] ReadEntriesFromIndex(IndexReader reader)
		{
			var results = new RavenJObject[reader.MaxDoc];
			using (var termDocs = reader.TermDocs())
			using (var termEnum = reader.Terms())
			{
				while (termEnum.Next())
				{
					var term = termEnum.Term;
					if (term == null)
						break;

					var text = term.Text;

					termDocs.Seek(termEnum);
					while (termDocs.Next())
					{
						RavenJObject result = results[termDocs.Doc];
						if (result == null)
							results[termDocs.Doc] = result = new RavenJObject();
						var propertyName = term.Field;
						if (propertyName.EndsWith("_ConvertToJson") ||
						    propertyName.EndsWith("_IsArray"))
							continue;
						if (propertyName == ("Cost_Range"))
						{
							if ((term.Text.Length == 6 && (term.Text[0] - 0x60) == 0) == false && // integer, first level
							    (term.Text.Length == 11 && (term.Text[0] - 0x20) == 0) == false) // long, first level
							{
								continue;
							}
						}
						if (result.ContainsKey(propertyName))
						{
							switch (result[propertyName].Type)
							{
								case JTokenType.Array:
									((RavenJArray) result[propertyName]).Add(text);
									break;
								case JTokenType.String:
									result[propertyName] = new RavenJArray
									{
										result[propertyName],
										text
									};
									break;
								default:
									throw new ArgumentException("No idea how to hanlde " + result[propertyName].Type);
							}
						}
						else
						{
							result[propertyName] = text;
						}
					}
				}
			}
			return results;
		}

	}
}