using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using Xunit.Sdk;

namespace Tests.Infrastructure
{
    public sealed class InlineDataWithRandomSeedAttribute : DataAttribute
    {
        private static int _runs;

        public InlineDataWithRandomSeedAttribute(params object[] dataValues)
        {
            DataValues = dataValues ?? new object[] { null };
        }

        public object[] DataValues { get; set; }

        public override IEnumerable<object[]> GetData(MethodInfo testMethod)
        {
            var objects = new object[DataValues.Length + 1];
            Array.Copy(DataValues, 0, objects, 0, DataValues.Length);

            // Using Environment.TickCount here is not appropiate, because
            // tests are run in a multithreaded environment and this value is
            // likely to be the same for all threads.
            Interlocked.Increment(ref _runs);
            var random = new Random(Environment.TickCount + _runs);
            objects[DataValues.Length] = random.Next();

            yield return objects;
        }
    }
}
