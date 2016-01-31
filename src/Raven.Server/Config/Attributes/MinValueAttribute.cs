// -----------------------------------------------------------------------
//  <copyright file="MinValueAttribute.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

namespace Raven.Server.Config.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
    public class MinValueAttribute : Attribute
    {
        public int Int32Value { get; set; }

        public MinValueAttribute(int value)
        {
            Int32Value = value;
        }
    }
}
