// -----------------------------------------------------------------------
//  <copyright file="NullableIntegerSettingWithMin.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Database.Config.Settings
{
    internal class NullableIntegerSettingWithMin : Setting<int?>
    {
        private readonly int minValue;

        public NullableIntegerSettingWithMin(string value, int? defaultValue, int minValue)
            : base(value, defaultValue)
        {
            this.minValue = minValue;
        }

        public NullableIntegerSettingWithMin(string value, Func<int?> getDefaultValue, int minValue)
            : base(value, getDefaultValue)
        {
            this.minValue = minValue;
        }

        public override int? Value
        {
            get
            {
                var val = string.IsNullOrEmpty(value) == false
                           ? int.Parse(value)
                           : (getDefaultValue != null ? getDefaultValue() : defaultValue);

                if (val.HasValue) 
                    return Math.Max(minValue, val.Value);

                return val;
            }
        }
    }
}
