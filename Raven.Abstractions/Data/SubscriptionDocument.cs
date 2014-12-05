// -----------------------------------------------------------------------
//  <copyright file="SubscriptionDocument.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Abstractions.Data
{
	public class SubscriptionDocument
	{
		public string Name { get; set; }
		public SubscriptionCriteria Criteria { get; set; }
		public Etag AckEtag { get; set; }
	}
}