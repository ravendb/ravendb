using System;
using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp
{
    public static class SnmpValuesHelper
    {
        public static TimeTicks TimeTicksMax = new TimeTicks(uint.MaxValue);

        public static TimeTicks TimeTicksZero = new TimeTicks(0);

        public static TimeSpan TimeSpanSnmpMax = TimeTicksMax.ToTimeSpan();
        
        public static TimeTicks TimeSpanToTimeTicks(TimeSpan timeSpan)
        {
            if (timeSpan <= TimeSpan.Zero)
                return TimeTicksZero;
            
            if (timeSpan > TimeSpanSnmpMax)
                return TimeTicksMax;
                    
            return new TimeTicks(timeSpan);
        }
        
    }
}
