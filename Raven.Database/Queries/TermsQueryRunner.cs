//-----------------------------------------------------------------------
// <copyright file="TermsQueryRunner.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Lucene.Net.Index;
using Lucene.Net.Search;

namespace Raven.Database.Queries
{
	public class TermsQueryRunner
	{
		private readonly DocumentDatabase database;

		public TermsQueryRunner(DocumentDatabase database)
		{
			this.database = database;
		}

		public ISet<string> GetTerms(string index, string field, string fromValue, int pageSize)
		{
			if(field == null) throw new ArgumentNullException("field");
			if(index == null) throw new ArgumentNullException("index");

			if (field.EndsWith("_Range"))
			{
				field = field.Substring(0, field.Length - "_Range".Length);
			}

			var result = new HashSet<string>();
			IndexSearcher currentIndexSearcher;
			using(database.IndexStorage.GetCurrentIndexSearcher(index, out currentIndexSearcher))
			{
				if(currentIndexSearcher == null)
				{
					throw new InvalidOperationException("Could not find current searcher");
				}
				using(var termEnum = currentIndexSearcher.IndexReader.Terms(new Term(field, fromValue ?? string.Empty)))
				{
					if (string.IsNullOrEmpty(fromValue) == false) // need to skip this value
					{
						while (termEnum.Term == null || fromValue.Equals(termEnum.Term.Text))
						{
							if (termEnum.Next() == false)
								return result;
						}
					}
					while (termEnum.Term == null || 
						field.Equals(termEnum.Term.Field))
					{
						if (termEnum.Term != null)
							result.Add(termEnum.Term.Text);

						if (result.Count >= pageSize)
							break;

						if (termEnum.Next() == false)
							break;
					}
				}
			}

			return result;
		}
	}
}
