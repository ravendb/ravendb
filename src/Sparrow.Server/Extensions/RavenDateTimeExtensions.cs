using System;

namespace Sparrow.Server.Extensions
{
    public static class RavenDateTimeExtensions
    {
        /// <summary>
        /// This function Processes the to string format of the form "yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff" for date times in
        /// invariant culture scenarios. This implementation takes 20% of the time of a regular .ToString(format) call
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="isUtc"></param>
        /// <returns></returns>
        public static unsafe ByteStringContext.InternalScope GetDefaultRavenFormat(this DateTime dt, ByteStringContext context, out ByteString value, bool isUtc = false)
        {
            Sparrow.Extensions.RavenDateTimeExtensions.ValidateDate(dt, isUtc);

            int size = 27 + (isUtc ? 1 : 0);
            var ticks = dt.Ticks;

            var scope = context.Allocate(size, out value);

            byte* ptr = value.Ptr;
            Sparrow.Extensions.RavenDateTimeExtensions.ProcessDefaultRavenFormat(ticks, ptr);
            ptr[size - 1] = (byte)'Z';

            return scope;
        }
    }
}
