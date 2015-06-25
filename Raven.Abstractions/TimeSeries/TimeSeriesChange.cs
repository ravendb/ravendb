// -----------------------------------------------------------------------
//  <copyright file="TimeSeriesChanges.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.TimeSeries
{
	public class TimeSeriesChange
	{
		public string Name { get; set; }
		public string Group { get; set; }
		public long Delta { get; set; }

		[JsonIgnore]
		public TaskCompletionSource<object> Done { get; set; }
	}
}