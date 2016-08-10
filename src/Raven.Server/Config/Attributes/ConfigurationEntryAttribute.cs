// -----------------------------------------------------------------------
//  <copyright file="ConfigurationEntryAttribute.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Runtime.CompilerServices;

namespace Raven.Server.Config.Attributes
{
    [AttributeUsage(AttributeTargets.Property)]
    public class ConfigurationEntryAttribute : Attribute
    {
        public string Key { get; set; }

        public ConfigurationEntryAttribute(string key, [CallerLineNumber]int order = 0) // the default order is the order of declaration in a configuration class
        {
            Key = key;
            Order = order;
        }

        public int Order { get; private set; }
    }
}
