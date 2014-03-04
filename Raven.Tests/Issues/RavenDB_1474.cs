// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1474.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Text;

using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_1474 : NoDisposalNeeded
    {
        public class Rate
        {
            public int Id { get; set; }
            public object Compoundings { get; set; }
        }

        public class RateWithCorrectCompoundingsType
        {
            public int Id { get; set; }
            public decimal Compoundings { get; set; }
        }

        private readonly Rate rate = new Rate { Compoundings = 12.166666666666666666666666667m };
        private readonly RateWithCorrectCompoundingsType rate2 = new RateWithCorrectCompoundingsType { Compoundings = 12.166666666666666666666666667m };

        /*
         * this will fail - in this scenario Json.Net deserializes Compoundings property value as double
                Assert.Equal() Failure
                Expected: 12.166666666666666666666666667
                Actual:   12.1666666666667		 
         */
        [Fact]
        public void Should_serialize_and_deserialize_correctly_as_decimal1()
        {
            var serializedRate = JsonConvert.SerializeObject(rate);
            var deserializedRate = JsonConvert.DeserializeObject<Rate>(serializedRate);
            Assert.Equal(Convert.ToDecimal(rate.Compoundings), Convert.ToDecimal(deserializedRate.Compoundings));
        }

        //this will fail for the same reason as previous test
        [Fact]
        public void Should_serialize_and_deserialize_correctly_as_decimal2()
        {
            var serializedRate = JsonConvert.SerializeObject(rate2);
            using (var rateStream = new MemoryStream(Encoding.UTF8.GetBytes(serializedRate)))
            using (var textReader = new StreamReader(rateStream, Encoding.UTF8))
            using (var jsonReader = new JsonTextReader(textReader))
            {
                var jObject = RavenJObject.Load(jsonReader);
                var compoundings = jObject.Value<decimal>("Compoundings");
                Assert.Equal(rate2.Compoundings, compoundings);
            }
        }
    }
}