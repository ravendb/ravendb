// -----------------------------------------------------------------------
//  <copyright file="SubscriptionCriteria.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Json.Linq;

namespace Raven.Abstractions.Data
{
	public class SubscriptionCriteria
	{
		public string KeyStartsWith { get; set; }

		public string BelongsToCollection { get; set; }

		public Dictionary<string, RavenJToken> PropertiesMatch { get; set; }

		public Dictionary<string, RavenJToken> PropertiesNotMatch { get; set; } 
	}
}