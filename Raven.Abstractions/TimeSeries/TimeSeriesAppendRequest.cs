// -----------------------------------------------------------------------
//  <copyright file="TimeSeriesAppendRequest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Abstractions.TimeSeries
{
	public class TimeSeriesAppendRequest
	{
		public double[] Values { get; set; } 
		
		public long Time { get; set; } 
	}
}