//-----------------------------------------------------------------------
// <copyright file="TermsQueryRunner.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
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

		public ISet<string> GetTerms(string index, string field, string fromValue, int pageSize, List<string> allTerms)
		{
			if(field == null) throw new ArgumentNullException("field");
			if(index == null) throw new ArgumentNullException("index");
			
			IndexSearcher currentIndexSearcher;
			using(database.IndexStorage.GetCurrentIndexSearcher(index, out currentIndexSearcher))
			{
				var termEnum = currentIndexSearcher.GetIndexReader().Terms(new Term(field, fromValue ?? string.Empty));
				try
				{
					if (string.IsNullOrEmpty(fromValue) == false) // need to skip this value
					{
						while (termEnum.Term() == null || fromValue.Equals(termEnum.Term().Text()))
						{
							if(termEnum.Next() == false)
							{
								allTerms.Sort();
								return new HashSet<string>(allTerms.Take(pageSize));
							}
						}
					}
					while (termEnum.Term() == null || 
						field.Equals(termEnum.Term().Field()))
					{
						if (termEnum.Term() != null)
							allTerms.Add(termEnum.Term().Text());

						if (termEnum.Next() == false)
							break;
					}
				}
				finally
				{
					termEnum.Close();
				}
			}

			allTerms.Sort();
			return new HashSet<string>(allTerms.Take(pageSize));
		}
	}
}