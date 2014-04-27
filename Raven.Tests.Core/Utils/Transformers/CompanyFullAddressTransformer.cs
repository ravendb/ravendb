// -----------------------------------------------------------------------
//  <copyright file="CompanyFullAddressTransformer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Core.Utils.Entities;

namespace Raven.Tests.Core.Utils.Transformers
{
	public class CompanyFullAddressTransformer : AbstractTransformerCreationTask<Company>
	{
		public class Result
		{
			public string FullAddress { get; set; }
		}

		public CompanyFullAddressTransformer()
		{
			TransformResults = companies => companies.Select(x => new
			{
				FullAddress = (x.Address1 ?? string.Empty) +
							  (x.Address2 != null ? (x.Address1 != null ? ", " : string.Empty) + x.Address2 : string.Empty) +
							  (x.Address3 != null ? (x.Address1 != null || x.Address2 != null ? ", " : string.Empty) + x.Address3 : string.Empty)
			});
		}
	}
}