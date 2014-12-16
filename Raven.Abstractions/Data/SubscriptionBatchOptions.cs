// -----------------------------------------------------------------------
//  <copyright file="SubscriptionBatchOptions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Abstractions.Data
{
	public class SubscriptionBatchOptions
	{
		public SubscriptionBatchOptions()
		{
			MaxDocCount = 4096;
			AcknowledgmentTimeout = TimeSpan.FromMinutes(1);
		}

		public int? MaxSize { get; set; }

		public int MaxDocCount { get; set; }

		public TimeSpan AcknowledgmentTimeout { get; set; }
	}
}