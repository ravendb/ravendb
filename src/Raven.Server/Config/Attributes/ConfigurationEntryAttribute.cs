// -----------------------------------------------------------------------
//  <copyright file="ConfigurationEntryAttribute.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Runtime.CompilerServices;

namespace Raven.Server.Config.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
    public class ConfigurationEntryAttribute : Attribute
    {
        public ConfigurationEntryAttribute(string key, ConfigurationEntryScope scope, [CallerLineNumber]int order = 0, bool setDefaultValueIfNeeded = true, bool isSecured = false) // the default order is the order of declaration in a configuration class
        {
            Key = key;
            Order = order;
            SetDefaultValueIfNeeded = setDefaultValueIfNeeded;
            IsSecured = isSecured;
            Scope = scope;
        }

        public readonly string Key;

        public readonly int Order;

        public readonly bool SetDefaultValueIfNeeded;

        public readonly bool IsSecured;

        public readonly ConfigurationEntryScope Scope;
    }

    public enum ConfigurationEntryScope
    {
        ServerWideOnly,
        ServerWideOrPerDatabase,
        ServerWideOrPerDatabaseOrPerIndex
    }
}
