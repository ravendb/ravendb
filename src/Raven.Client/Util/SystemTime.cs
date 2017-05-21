//-----------------------------------------------------------------------
// <copyright company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.Client.Util
{
    public class SystemTime
    {
        private static readonly SystemTime Instance = new SystemTime();

        /// <summary>
        /// Tests now run in parallel so this is no longer static to mitigate the possibility of getting incorrect results. Use DocumentDatabase.Time instead.
        /// </summary>
        public Func<DateTime> UtcDateTime;

        public Action<int> WaitCalled;

        public DateTime GetUtcNow()
        {
            var temp = UtcDateTime;
            return temp?.Invoke() ?? DateTime.UtcNow;
        }

        public static DateTime UtcNow => Instance.GetUtcNow();
    }
}
