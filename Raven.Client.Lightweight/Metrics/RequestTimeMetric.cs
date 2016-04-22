// -----------------------------------------------------------------------
//  <copyright file="RequestTimeMetric.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Raven.Client.Metrics
{
    public interface IRequestTimeMetric
    {
        void Update(long requestTimeInMilliseconds);

        bool RateSurpassed(ConventionBase conventions);

        double Rate();
    }

    public class DecreasingTimeMetric : IRequestTimeMetric
    {
        internal readonly IRequestTimeMetric RequestTimeMetric;

        private const double MaxDecreasingRatio = 0.75;

        private const double MinDecreasingRatio = 0.25;

        public DecreasingTimeMetric(IRequestTimeMetric requestTimeMetric)
        {
            RequestTimeMetric = requestTimeMetric;
        }

        public void Update(long requestTimeInMilliseconds)
        {
            var rate = RequestTimeMetric.Rate();
            var maxRate = MaxDecreasingRatio * rate;
            var minRate = MinDecreasingRatio * rate;

            var decreasingRate = rate - requestTimeInMilliseconds;

            if (decreasingRate > maxRate)
                decreasingRate = maxRate;

            if (decreasingRate < minRate)
                decreasingRate = minRate;

            RequestTimeMetric.Update((long)decreasingRate);
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
            var requestTimeSlaThresholdInMilliseconds = conventions.RequestTimeSlaThresholdInMilliseconds;
            var rate = Rate();

            if (surpassed)
                return surpassed = rate >= SwitchBackRatio * requestTimeSlaThresholdInMilliseconds;

            return surpassed = rate >= requestTimeSlaThresholdInMilliseconds;
        }

        public double Rate()
        {
            return ewma.Rate(TimeUnit.Milliseconds);
        }
    }

    public class ComplexTimeMetric : IRequestTimeMetric
    {
        private static readonly RequestTimeMetricEqualityComparer Comparer = new RequestTimeMetricEqualityComparer();

        private readonly HashSet<IRequestTimeMetric> previous = new HashSet<IRequestTimeMetric>(Comparer);

        private IRequestTimeMetric current;

        public void AddCurrent(IRequestTimeMetric requestTimeMetric)
        {
            Debug.Assert(requestTimeMetric is RequestTimeMetric);

            if (current == requestTimeMetric)
                return;

            if (current == null)
            {
                current = requestTimeMetric;
                return;
            }

            previous.Add(new DecreasingTimeMetric(current));
            previous.Remove(requestTimeMetric);
            current = requestTimeMetric;
        }

        public void Update(long requestTimeInMilliseconds)
        {
            current?.Update(requestTimeInMilliseconds);

            foreach (var metric in previous)
                metric.Update(requestTimeInMilliseconds);
        }

        public bool RateSurpassed(ConventionBase conventions)
        {
            var local = current;
            if (local == null)
                return false;

            return local.RateSurpassed(conventions);
        }

        public double Rate()
        {
            var local = current;
            if (local == null)
                return 0;

            return local.Rate();
        }

        private class RequestTimeMetricEqualityComparer : IEqualityComparer<IRequestTimeMetric>
        {
            public bool Equals(IRequestTimeMetric x, IRequestTimeMetric y)
            {
                if (x == null && y == null)
                    return true;

                if (x == null || y == null)
                    return false;

                if (ReferenceEquals(x, y))
                    return true;

                var xDecreasing = x as DecreasingTimeMetric;
                var yDecreasing = y as DecreasingTimeMetric;

                var xMetric = xDecreasing != null ? xDecreasing.RequestTimeMetric : x;
                var yMetric = yDecreasing != null ? yDecreasing.RequestTimeMetric : y;

                return xMetric.Equals(yMetric);
            }

            public int GetHashCode(IRequestTimeMetric obj)
            {
                var decreasing = obj as DecreasingTimeMetric;
                var metric = decreasing != null ? decreasing.RequestTimeMetric : obj;

                return metric.GetHashCode();
            }
        }
    }
}
