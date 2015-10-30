// -----------------------------------------------------------------------
//  <copyright file="ConfigurationEntryAttribute.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Database.Config.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
    public class ConfigurationEntryAttribute : Attribute
    {
        public string Key { get; set; }

        public ConfigurationEntryAttribute(string key)
        {
            Key = key;
        }
    }
}
