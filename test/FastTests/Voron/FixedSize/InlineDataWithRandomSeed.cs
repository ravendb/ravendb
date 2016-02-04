// -----------------------------------------------------------------------
//  <copyright file="InlineDataWithRandomSeed.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Reflection;
using Xunit;
using Xunit.Sdk;

namespace Voron.Tests.FixedSize
{
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
    public class InlineDataWithRandomSeed : DataAttribute
    {
        public InlineDataWithRandomSeed(params object[] dataValues)
        {
            this.DataValues = dataValues ?? new object[] {null};
        }

        public object[] DataValues { get; set; }

        public override IEnumerable<object[]> GetData(MethodInfo testMethod)
        {
            var objects = new object[DataValues.Length + 1];
            Array.Copy(DataValues, 0, objects, 0, DataValues.Length);
            objects[DataValues.Length] = Environment.TickCount;
            yield return objects;
        }
    }
}
