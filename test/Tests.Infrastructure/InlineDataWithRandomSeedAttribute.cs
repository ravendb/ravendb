using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Sdk;
using Xunit.v3;

namespace Tests.Infrastructure
{
    public sealed class InlineDataWithRandomSeedAttribute : DataAttribute
    {
        public override bool SupportsDiscoveryEnumeration() => false;

        private static int _runs;

        public InlineDataWithRandomSeedAttribute(params object[] dataValues)
        {
            DataValues = dataValues ?? new object[] { null };
        }

        public object[] DataValues { get; set; }

        public override ValueTask<IReadOnlyCollection<ITheoryDataRow>> GetData(MethodInfo testMethod, DisposalTracker disposalTracker)
        {
            var objects = new object[DataValues.Length + 1];
            Array.Copy(DataValues, 0, objects, 0, DataValues.Length);

            Interlocked.Increment(ref _runs);
            var random = new Random(Environment.TickCount + _runs);
            objects[DataValues.Length] = random.Next();

            var result = new List<ITheoryDataRow> { new TheoryDataRow(objects) };
            return new ValueTask<IReadOnlyCollection<ITheoryDataRow>>(result);
        }
    }
}
