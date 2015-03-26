// -----------------------------------------------------------------------
//  <copyright file="RequestTimeMetric.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Client.Metrics
{
	public class RequestTimeMetric
	{
		private readonly EWMA ewma;

		public RequestTimeMetric()
		{
			ewma = new EWMA(EWMA.M1Alpha, 1, TimeUnit.Milliseconds);
		}

		public void Update(long requestTimeInMilliseconds)
		{
			ewma.Update(requestTimeInMilliseconds);
			ewma.Tick();
		}

		public double Rate()
		{
			return ewma.Rate(TimeUnit.Milliseconds);
		}
	}
}