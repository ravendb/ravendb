// -----------------------------------------------------------------------
//  <copyright file="TimeUnitAttribute.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Raven.Server.Config.Settings;

namespace Raven.Server.Config.Attributes
{
    [AttributeUsage(AttributeTargets.Property)]
    public class TimeUnitAttribute : Attribute
    {
        public TimeUnit Unit { get; set; }

        public TimeUnitAttribute(TimeUnit unit)
        {
            Unit = unit;
        }
    }
}
