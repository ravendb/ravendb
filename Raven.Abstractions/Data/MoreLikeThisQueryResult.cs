using System;
using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
	public class MoreLikeThisQueryResult
	{
		public MoreLikeThisQueryResult()
		{
			
		}

		public MultiLoadResult Result { get; set; }
		public Guid Etag { get; set; }
	}
}