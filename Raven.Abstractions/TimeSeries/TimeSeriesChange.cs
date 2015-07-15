// -----------------------------------------------------------------------
//  <copyright file="TimeSeriesChanges.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.TimeSeries
{
	public class TimeSeriesAppend
	{
		public string Type { get; set; }
		public string Key { get; set; }
		public DateTime At { get; set; }
		public double[] Values { get; set; }

		[JsonIgnore]
		public TaskCompletionSource<object> Done { get; set; }
	}

	public class TimeSeriesDelete
	{
		public string Key { get; set; }

		[JsonIgnore]
		public TaskCompletionSource<object> Done { get; set; }
	}

	public class TimeSeriesDeleteRange
	{
		public string Key { get; set; }
		public DateTime Start { get; set; }
		public DateTime End { get; set; }

		[JsonIgnore]
		public TaskCompletionSource<object> Done { get; set; }
	}
}