// -----------------------------------------------------------------------
//  <copyright file="RequestTimeMetric.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.NewClient.Client.Metrics
{
    public interface IRequestTimeMetric
    {
        void Update(long requestTimeInMilliseconds);

        bool RateSurpassed(ConventionBase conventions);

        double Rate();
    }

    public class DecreasingTimeMetric : IRequestTimeMetric
    {
        private readonly IRequestTimeMetric requestTimeMetric;

        private const double MaxDecreasingRatio = 0.75;

        private const double MinDecreasingRatio = 0.25;

        public DecreasingTimeMetric(IRequestTimeMetric requestTimeMetric)
        {
            this.requestTimeMetric = requestTimeMetric;
        }

        public void Update(long requestTimeInMilliseconds)
        {
            var rate = requestTimeMetric.Rate();
            var maxRate = MaxDecreasingRatio * rate;
            var minRate = MinDecreasingRatio * rate;

            var decreasingRate = rate - requestTimeInMilliseconds;

            if (decreasingRate > maxRate)
                decreasingRate = maxRate;

            if (decreasingRate < minRate)
                decreasingRate = minRate;

            requestTimeMetric.Update((long)decreasingRate);
        }

        public bool RateSurpassed(ConventionBase conventions)
        {
            throw new NotSupportedException();
        }

        public double Rate()
        {
            throw new NotSupportedException();
        }
    }

    public class RequestTimeMetric : IRequestTimeMetric
    {
        private readonly EWMA ewma;

        private const double SwitchBackRatio = 0.75;

        private volatile bool surpassed;

        public RequestTimeMetric()
        {
            ewma = new EWMA(EWMA.M1Alpha, 1, TimeUnit.Milliseconds);

            for (var i = 0; i < 60; i++)
                Update(0);
        }

        public void Update(long requestTimeInMilliseconds)
        {
            ewma.Update(requestTimeInMilliseconds);
            ewma.Tick();
        }

        public bool RateSurpassed(ConventionBase conventions)
        {
            var requestTimeThresholdInMilliseconds = conventions.RequestTimeThresholdInMilliseconds;
            var rate = Rate();

            if (surpassed)
                return surpassed = rate >= SwitchBackRatio * requestTimeThresholdInMilliseconds;

            return surpassed = rate >= requestTimeThresholdInMilliseconds;
        }

        public double Rate()
        {
            return ewma.Rate(TimeUnit.Milliseconds);
        }
    }
}
