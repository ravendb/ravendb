//-----------------------------------------------------------------------
// <copyright file="Companies_ByRegion.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;

namespace Raven.Samples.Includes
{
	public class Companies_ByRegion : AbstractIndexCreationTask<Company>
	{
		public Companies_ByRegion()
		{
			Map = companies => from company in companies
							   select new {company.Region};
		}
	}
}
