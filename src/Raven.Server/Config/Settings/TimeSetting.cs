// -----------------------------------------------------------------------
//  <copyright file="TimeSetting.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

namespace Raven.Server.Config.Settings
{
    public struct TimeSetting
    {
        public TimeSpan AsTimeSpan;

        public TimeSetting(long value, TimeUnit unit)
        {
            if (value < 0)
            {
                if (value != -1)
                    throw new ArgumentOutOfRangeException(nameof(value), value, "TimeSetting does not support negative values. The only negative value supported is -1 (TimeSpan.MaxValue).");

                AsTimeSpan = TimeSpan.MaxValue;
                return;
            }

            switch (unit)
            {
                case TimeUnit.Milliseconds:
                    AsTimeSpan = TimeSpan.FromMilliseconds(value);
                    break;
                case TimeUnit.Seconds:
                    AsTimeSpan = TimeSpan.FromSeconds(value);
                    break;
                case TimeUnit.Minutes:
                    AsTimeSpan = TimeSpan.FromMinutes(value);
                    break;
                case TimeUnit.Hours:
                    AsTimeSpan = TimeSpan.FromHours(value);
                    break;
                case TimeUnit.Days:
                    AsTimeSpan = TimeSpan.FromDays(value);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(unit), unit, "Unknown TimeUnit value");
            }
        }
    }

    public enum TimeUnit
    {
        Milliseconds,
        Seconds,
        Minutes,
        Hours,
        Days
    }
}
