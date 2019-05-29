//-----------------------------------------------------------------------
// <copyright file="TimeSeriesValue.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.Client.Documents.Session
{
    public class TimeSeriesValue
    {
        public DateTime Timestamp;
        public double[] Values;
        public string Tag;
    }
}
