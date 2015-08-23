// -----------------------------------------------------------------------
//  <copyright file="InlineDataWithRandomSeed.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Sparrow;
using Voron.Util;
using Xunit.Extensions;

namespace Voron.Tests.FixedSize
{
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
    public class InlineDataWithRandomSeed : DataAttribute
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="InlineDataAttribute" /> class.
        /// </summary>
        /// <param name="dataValues">The data values to pass to the theory</param>
        public InlineDataWithRandomSeed(params object[] dataValues)
        {
            this.DataValues = dataValues ?? new object[] {null};
        }

        /// <summary>
        ///     Gets the data values.
        /// </summary>
        public object[] DataValues { get; set; }

        /// <summary>
        ///     Returns the data to be used to test the theory.
        /// </summary>
        /// <param name="methodUnderTest">The method that is being tested</param>
        /// <param name="parameterTypes">The types of the parameters for the test method</param>
        /// <returns>The theory data, in table form</returns>
        public override IEnumerable<object[]> GetData(MethodInfo methodUnderTest, Type[] parameterTypes)
        {
            var objects = new object[DataValues.Length+1];
            Array.Copy(DataValues,0,objects,0, DataValues.Length);
            objects[DataValues.Length] = Environment.TickCount;
            yield return objects;
            
        }
    }
}