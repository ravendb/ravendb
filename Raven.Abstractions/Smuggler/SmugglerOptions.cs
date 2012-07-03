//-----------------------------------------------------------------------
// <copyright file="ExportSpec.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Json;
using Raven.Json.Linq;

namespace Raven.Abstractions.Smuggler
{
	public class SmugglerOptions
	{
		public SmugglerOptions()
		{
			Filters = new Dictionary<string, string>();
			OperateOnTypes = ItemType.Indexes | ItemType.Documents | ItemType.Attachments;
			Timeout = 30 * 1000; // 30 seconds
			BatchSize = 1024;
		}

		/// <summary>
		/// A file to write to when doing an export or read from when doing an import.
		/// </summary>
		public string File { get; set; }

		public Dictionary<string, string> Filters { get; set; }

		/// <summary>
		/// Specify the types to operate on. You can specify more than one type by combining items with the OR parameter.
		/// Default is all items.
		/// Usage example: OperateOnTypes = ItemType.Indexes | ItemType.Documents | ItemType.Attachments.
		/// </summary>
		public ItemType OperateOnTypes { get; set; }

		/// <summary>
		/// The timeout for requests
		/// </summary>
		public int Timeout { get; set; }

		/// <summary>
		/// The batch size for loading to ravendb
		/// </summary>
		public int BatchSize { get; set; }

		public bool MatchFilters(RavenJToken item)
		{
			foreach (var filter in Filters)
			{
				var copy = filter;
				foreach (var tuple in item.SelectTokenWithRavenSyntaxReturningFlatStructure(copy.Key))
				{
					if (tuple == null || tuple.Item1 == null)
						continue;
					var val = tuple.Item1.Type == JTokenType.String
								? tuple.Item1.Value<string>()
								: tuple.Item1.ToString(Formatting.None);
					if (string.Equals(val, filter.Value, StringComparison.InvariantCultureIgnoreCase) == false)
						return false;
				}
			}
			return true;
		}
	}

	[Flags]
	public enum ItemType
	{
		Documents,
		Indexes,
		Attachments,
	}
}
