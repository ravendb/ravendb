//-----------------------------------------------------------------------
// <copyright file="ExportSpec.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Abstractions.Json;
using Raven.Json.Linq;

namespace Raven.Smuggler
{
	public class SmugglerOptions
	{
		public SmugglerOptions()
		{
			IncludeAttachments = true;
			Filters = new Dictionary<string, string>();
		}

		public SmugglerAction Action { get; set; }

		public string File { get; set; }

		public Dictionary<string, string> Filters { get; set; }

		/// <summary>
		/// Export indexes only. This is supported for export action only.
		/// Default if false.
		/// </summary>
		public bool ExportIndexesOnly { get; set; }

		/// <summary>
		/// Include attachments in the export. This is supported for export action only.
		/// Default is true.
		/// </summary>
		public bool IncludeAttachments { get; set; }

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

	public enum SmugglerAction
	{
		Import = 1,
		Export,
	}
}