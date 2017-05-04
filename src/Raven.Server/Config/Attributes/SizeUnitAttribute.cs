// -----------------------------------------------------------------------
//  <copyright file="SizeUnitAttribute.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Sparrow;

namespace Raven.Server.Config.Attributes
{
    [AttributeUsage(AttributeTargets.Property)]
    public class SizeUnitAttribute : Attribute
    {
        public SizeUnit Unit { get; set; }

        public SizeUnitAttribute(SizeUnit unit)
        {
            Unit = unit;
        }
    }
}
