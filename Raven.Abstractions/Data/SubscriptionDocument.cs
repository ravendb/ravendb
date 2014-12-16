// -----------------------------------------------------------------------
//  <copyright file="SubscriptionDocument.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Abstractions.Data
{
	public class SubscriptionDocument
	{
		public long SubscriptionId { get; set; }
		public SubscriptionCriteria Criteria { get; set; }
		public Etag AckEtag { get; set; }
		public DateTime LastSentBatchTime { get; set; }
	}
}