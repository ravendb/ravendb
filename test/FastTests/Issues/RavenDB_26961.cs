using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_26961 : NoDisposalNeeded
    {
        public RavenDB_26961(ITestOutputHelper output) : base(output)
        {
        }

        private enum Color
        {
            Red,
            Blue
        }

        private class Item
        {
            public string Name { get; set; }
            public Color Color { get; set; }
            public int Kind { get; set; }
            public DateTime When { get; set; }
        }

        private class Holder
        {
            public string Field;
        }

        // Captured locals are closure fields; extracting their value must read the field, not compile
        // an expression per call. The probe is allocation, not JitInfo: JitInfo does not count the
        // DynamicMethods that Expression.Compile emits.
        [RavenFact(RavenTestCategory.Querying)]
        public void Extracting_captured_locals_does_not_compile_an_expression_per_call()
        {
            var name = "joe";
            var prefix = "j";
            var color = Color.Blue;
            var holder = new Holder { Field = "joe" };

            var stringLocal = Rhs(x => x.Name == name);                 // FieldInfo on closure
            var nestedLocal = Rhs(x => x.Name == holder.Field);         // recursive member access
            var staticField = Rhs(x => x.When > DateTime.MinValue);     // static field, null target
            var enumLocal = Rhs(x => x.Color == color);                 // Convert(enum field, int), target type is the enum
            var enumCastToInt = Rhs(x => x.Kind == (int)color);         // Convert(enum field, int), target type is int
            var methodCall = Rhs(x => x.Name == string.Concat(prefix, "oe")); // Call branch (method invocation)

            var provider = new LinqPathProvider(new DocumentConventions());

            Assert.Equal("joe", provider.GetValueFromExpression(stringLocal, typeof(string)));
            Assert.Equal("joe", provider.GetValueFromExpression(nestedLocal, typeof(string)));
            Assert.Equal(DateTime.MinValue, provider.GetValueFromExpression(staticField, typeof(DateTime)));
            Assert.Equal("Blue", provider.GetValueFromExpression(enumLocal, typeof(Color)));
            Assert.Equal("joe", provider.GetValueFromExpression(methodCall, typeof(string)));
            // Convert(enum -> int) with an int target must yield the int, not the enum: a value-preserving
            // shortcut that returned the operand would pass the enum==enum case but break this one.
            Assert.Equal(1, provider.GetValueFromExpression(enumCastToInt, typeof(int)));

            const int iterations = 10_000;

            for (int i = 0; i < 50; i++) // warm up so first-call JIT is excluded from the measurement
                Extract(provider, stringLocal, nestedLocal, staticField);

            long before = GC.GetAllocatedBytesForCurrentThread();

            for (int i = 0; i < iterations; i++)
                Extract(provider, stringLocal, nestedLocal, staticField);

            long perIteration = (GC.GetAllocatedBytesForCurrentThread() - before) / iterations;
            Output.WriteLine($"{perIteration} bytes allocated per iteration (3 field extractions)");

            // reflection reads allocate ~nothing; a per-call compile would allocate orders of magnitude more.
            Assert.True(perIteration < 512, $"{perIteration} bytes/iteration - captured-local field extraction is still compiling expressions");
        }

        private static void Extract(LinqPathProvider provider, Expression s, Expression n, Expression d)
        {
            provider.GetValueFromExpression(s, typeof(string));
            provider.GetValueFromExpression(n, typeof(string));
            provider.GetValueFromExpression(d, typeof(DateTime));
        }

        private static Expression Rhs(Expression<Func<Item, bool>> predicate) => ((BinaryExpression)predicate.Body).Right;
    }
}
