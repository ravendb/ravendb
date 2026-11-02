using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_26440 : RavenTestBase
    {
        public RavenDB_26440(ITestOutputHelper output) : base(output)
        {
        }

        private const string Tag = "a";

        // a chain of n calls costs n * (n - 1) / 2, see StackDepthRetriever.GetMaxInvocationNestingCost()
        private static int CostOfChain(int chainDepth) => chainDepth * (chainDepth - 1) / 2;

        [RavenFact(RavenTestCategory.Indexes)]
        public void Map_With_Method_Call_Chain_Above_The_Limit_Should_Fail_Compilation()
        {
            using (var store = GetDocumentStore())
            {
                var chainDepth = IndexCompiler.MaxAllowedInvocationChainDepth + 1;
                var indexDefinition = new IndexDefinition
                {
                    Name = "DeepChainIndex",
                    Maps = { CreateMapWithConcatChains(numberOfChains: 1, chainDepth) }
                };

                var ex = Assert.Throws<IndexCompilationException>(() => store.Maintenance.Send(new PutIndexesOperation(indexDefinition)));
                Assert.Contains($"The map function nests its method calls too deeply: their combined nesting cost is {CostOfChain(chainDepth)}, while the maximum is {IndexCompiler.MaxAllowedInvocationNestingCost}", ex.Message);
                Assert.Equal(nameof(IndexDefinition.Maps), ex.IndexDefinitionProperty);
                Assert.Equal(indexDefinition.Maps.Single(), ex.ProblematicText);
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void Reduce_With_Method_Call_Chain_Above_The_Limit_Should_Fail_Compilation()
        {
            using (var store = GetDocumentStore())
            {
                var chainDepth = IndexCompiler.MaxAllowedInvocationChainDepth + 1;
                var concatChain = string.Concat(Enumerable.Repeat(".Concat(g.SelectMany(x => x.Tags))", chainDepth - 2)); // plus the opening SelectMany() and the closing ToArray()
                var indexDefinition = new IndexDefinition
                {
                    Name = "DeepChainMapReduceIndex",
                    Maps = { "from user in docs.Users select new { user.Name, user.Tags }" },
                    Reduce = $"from result in results group result by result.Name into g select new {{ Name = g.Key, Tags = g.SelectMany(x => x.Tags){concatChain}.ToArray() }}"
                };

                var ex = Assert.Throws<IndexCompilationException>(() => store.Maintenance.Send(new PutIndexesOperation(indexDefinition)));
                Assert.Contains("The reduce function nests its method calls too deeply", ex.Message);
                Assert.Equal(nameof(IndexDefinition.Reduce), ex.IndexDefinitionProperty);
                Assert.Equal(indexDefinition.Reduce, ex.ProblematicText);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [InlineData(1, IndexCompiler.MaxAllowedInvocationChainDepth)] // exactly the budget
        [InlineData(2, 90)] // 2 * 4005 = 8010 out of 8128
        public void Map_With_Method_Call_Chains_Within_The_Budget_Should_Compile_And_Index(int numberOfChains, int chainDepth)
        {
            Assert.True(numberOfChains * CostOfChain(chainDepth) <= IndexCompiler.MaxAllowedInvocationNestingCost);

            using (var store = GetDocumentStore())
            {
                var indexDefinition = new IndexDefinition
                {
                    Name = "DeepChainIndex",
                    Maps = { CreateMapWithConcatChains(numberOfChains, chainDepth) }
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "John", Tags = new List<string> { Tag, "b" } });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var indexErrors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexDefinition.Name }));
                Assert.Empty(indexErrors[0].Errors);

                using (var session = store.OpenSession())
                {
                    var users = session.Advanced.DocumentQuery<User>(indexDefinition.Name)
                        .WhereEquals("Values", Tag)
                        .ToList();

                    Assert.Equal(1, users.Count);
                    Assert.Equal("John", users[0].Name);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [InlineData(2, IndexCompiler.MaxAllowedInvocationChainDepth)]
        [InlineData(3, 90)]
        public void Map_With_Several_Method_Call_Chains_Below_The_Limit_That_Together_Overflow_The_Stack_Should_Fail_Compilation(int numberOfChains, int chainDepth)
        {
            // every chain in a single map is generated into the same method (the Map_0 iterator), so their stack usage adds up:
            // measured on x64 (default stack) with a '?? Enumerable.Empty<string>()' argument per call, 1 x 128 and 4 x 64 calls index fine
            // while 2 x 128 and 3 x 90 calls crash the indexing thread with a StackOverflowException
            // no documents are stored on purpose, if the index is wrongly accepted the test fails on the assertion instead of crashing the test process
            using (var store = GetDocumentStore())
            {
                var indexDefinition = new IndexDefinition
                {
                    Name = "SeveralChainsIndex",
                    Maps = { CreateMapWithConcatChains(numberOfChains, chainDepth, argument: "user.Tags ?? Enumerable.Empty<string>()") }
                };

                var ex = Assert.Throws<IndexCompilationException>(() => store.Maintenance.Send(new PutIndexesOperation(indexDefinition)));
                Assert.Contains("The map function nests its method calls too deeply", ex.Message);
                Assert.Equal(nameof(IndexDefinition.Maps), ex.IndexDefinitionProperty);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [InlineData("ConditionalAccess", 40)]
        [InlineData("Parenthesized", 200)]
        [InlineData("DynamicCast", 200)]
        [InlineData("NestedArguments", 200)]
        public void Map_With_Method_Calls_Split_Into_Short_Chains_That_Overflow_The_Stack_Should_Fail_Compilation(string shape, int numberOfCalls)
        {
            // none of these is a plain chain of calls on one receiver, but the generated code still overflows the stack of the indexing thread
            // (measured on x64, default stack: every shape below crashed the server in the generated Map_0 once a document was indexed)
            // '?.' is the worst case: ConditionalAccessExpressionRewriter copies the whole chain before it at every step, so 40 links are enough
            // no documents are stored on purpose, if the index is wrongly accepted the test fails on the assertion instead of crashing the test process
            const string source = "(user.Tags ?? Enumerable.Empty<string>())";

            var expression = source;
            for (var i = 0; i < numberOfCalls; i++)
            {
                expression = shape switch
                {
                    "ConditionalAccess" => $"{expression}?.Concat({source})",
                    "Parenthesized" => $"({expression}.Concat({source}))",
                    "DynamicCast" => $"((dynamic){expression}.Concat({source}))",
                    "NestedArguments" => $"{source}.Concat({expression})",
                    _ => throw new ArgumentOutOfRangeException(nameof(shape), shape, null)
                };
            }

            using (var store = GetDocumentStore())
            {
                var indexDefinition = new IndexDefinition
                {
                    Name = "SplitChainIndex",
                    Maps = { $"from user in docs.Users select new {{ Values = {expression} }}" }
                };

                var ex = Assert.Throws<IndexCompilationException>(() => store.Maintenance.Send(new PutIndexesOperation(indexDefinition)));
                Assert.Contains("The map function nests its method calls too deeply", ex.Message);
                Assert.Equal(nameof(IndexDefinition.Maps), ex.IndexDefinitionProperty);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [InlineData(IndexCompiler.MaxAllowedInvocationChainDepth - 1, false)] // 8001 + the 3 calls around the lambda
        [InlineData(IndexCompiler.MaxAllowedInvocationChainDepth, true)] // 8128 + 3, over the budget
        public void Method_Call_Chain_Inside_A_Lambda_Adds_Up_With_The_Calls_Around_It(int chainDepth, bool shouldFail)
        {
            // a lambda is compiled into a method of its own, but it runs on top of the frame of Map_0, so the chain in its body is costed as a scope
            // of its own and then added to the cost of the scope around it: 'user.Tags.Select(...).SelectMany(...).ToArray()' costs 0 + 1 + 2 = 3
            using (var store = GetDocumentStore())
            {
                var chain = "user.Tags" + string.Concat(Enumerable.Repeat(".Concat(user.Tags)", chainDepth - 1)) + ".ToArray()"; // 'chainDepth' calls, the ToArray() included
                var indexDefinition = new IndexDefinition
                {
                    Name = "ChainInLambdaIndex",
                    Maps = { $"from user in docs.Users select new {{ Values = user.Tags.Select(t => {chain}).SelectMany(x => x).ToArray() }}" }
                };

                if (shouldFail)
                {
                    var ex = Assert.Throws<IndexCompilationException>(() => store.Maintenance.Send(new PutIndexesOperation(indexDefinition)));
                    Assert.Contains("The map function nests its method calls too deeply", ex.Message);
                    return;
                }

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "John", Tags = new List<string> { Tag, "b" } });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var indexErrors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexDefinition.Name }));
                Assert.Empty(indexErrors[0].Errors);
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void Map_With_An_Extremely_Deep_Method_Call_Chain_Should_Fail_Compilation_Without_Crashing()
        {
            // the first gate on the parsed tree rejects it before NormalizeWhitespace(), which recurses over the tree without a stack guard
            using (var store = GetDocumentStore())
            {
                var indexDefinition = new IndexDefinition
                {
                    Name = "ExtremelyDeepChainIndex",
                    Maps = { CreateMapWithConcatChains(numberOfChains: 1, chainDepth: 10_000) }
                };

                var ex = Assert.Throws<IndexCompilationException>(() => store.Maintenance.Send(new PutIndexesOperation(indexDefinition)));
                Assert.Contains("The map function nests its method calls too deeply", ex.Message);
                Assert.Equal(nameof(IndexDefinition.Maps), ex.IndexDefinitionProperty);
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void Map_With_Nested_Lambdas_Each_Within_The_Budget_That_Together_Overflow_The_Stack_Should_Fail_Compilation()
        {
            // a nested lambda runs while the frames of Map_0 and of every enclosing lambda are still on the stack, so their costs add up along the nesting
            // measured on x64 (default stack): 8 nested scopes with a 32-link '?.' chain each are accepted, and the indexing thread overflows at the 5th frame
            // no documents are stored on purpose, if the index is wrongly accepted the test fails on the assertion instead of crashing the test process
            const int scopes = 8;
            const int links = 32;

            var chain = "user.Tags" + string.Concat(Enumerable.Repeat("?.Concat(user.Tags)", links)) + "?.ToArray()";
            var body = chain;
            for (var level = scopes - 1; level >= 1; level--)
                body = $"new {{ A = {chain}, B = user.Tags.Select(t{level} => {body}).ToArray() }}";

            using (var store = GetDocumentStore())
            {
                var indexDefinition = new IndexDefinition
                {
                    Name = "NestedLambdasIndex",
                    Maps = { $"from user in docs.Users select {body}" }
                };

                var ex = Assert.Throws<IndexCompilationException>(() => store.Maintenance.Send(new PutIndexesOperation(indexDefinition)));
                Assert.Contains("The map function nests its method calls too deeply", ex.Message);
                Assert.Equal(nameof(IndexDefinition.Maps), ex.IndexDefinitionProperty);
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void Map_With_An_Extremely_Deep_Expression_Without_Method_Calls_Should_Be_Rejected_By_The_First_Gate()
        {
            // NormalizeWhitespace() recurses over the tree without Roslyn's stack guard (SyntaxNormalizer.GetDeclarationDepth), so when this map is put
            // through the server it crashes the whole process with a StackOverflowException (measured on x64: 3000 terms, 3 out of 3 runs)
            // the compiler is called directly on a thread with a large stack here, so without the gate the test fails on the assertion instead of crashing
            var indexDefinition = new IndexDefinition
            {
                Name = "DeepExpressionIndex",
                Maps = { $"from user in docs.Users select new {{ Values = {string.Join(" + ", Enumerable.Repeat("user.Name", 3000))} }}" }
            };

            Exception exception = null;
            var thread = new Thread(() =>
            {
                try
                {
                    IndexCompiler.Compile(indexDefinition, IndexDefinitionBaseServerSide.IndexVersion.CurrentVersion);
                }
                catch (Exception e)
                {
                    exception = e;
                }
            }, maxStackSize: 256 * 1024 * 1024);
            thread.Start();
            thread.Join();

            var ex = Assert.IsType<IndexCompilationException>(exception);
            Assert.Contains("The map function is nested too deeply", ex.Message);
            Assert.Equal(nameof(IndexDefinition.Maps), ex.IndexDefinitionProperty);
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void Reduce_Validation_Error_Should_Keep_IndexDefinitionProperty_And_ProblematicText()
        {
            // the validation errors thrown by the reduce rewriters carry neither, the generic catch in HandleReduce has to add them
            using (var store = GetDocumentStore())
            {
                var indexDefinition = new IndexDefinition
                {
                    Name = "CountInGroupingIndex",
                    Maps = { "from i in docs.Items select new { i.Year, i.Number, Count = 0 }" },
                    Reduce = "from r in results group r by new { r.Year, r.Number } into g select new { Year = g.Key.Year, Number = g.Key.Number, Count = g.Count() }"
                };

                var ex = Assert.Throws<IndexCompilationException>(() => store.Maintenance.Send(new PutIndexesOperation(indexDefinition)));
                Assert.Contains("Reduce cannot contain Count() methods in grouping", ex.Message);
                Assert.Equal(nameof(IndexDefinition.Reduce), ex.IndexDefinitionProperty);
                Assert.NotNull(ex.ProblematicText);
            }
        }

        private static string CreateMapWithConcatChains(int numberOfChains, int chainDepth, string argument = "user.Tags")
        {
            // each chain is '(argument).Concat(argument)...Concat(argument).ToArray()' with exactly 'chainDepth' calls, the closing ToArray() included,
            // and the chains are the fields 'Values', 'Values1', ... of the same map
            var concatChain = string.Concat(Enumerable.Repeat($".Concat({argument})", chainDepth - 1));
            var fields = Enumerable.Range(0, numberOfChains).Select(i => $"Values{(i == 0 ? string.Empty : i.ToString())} = ({argument}){concatChain}.ToArray()");
            return $"from user in docs.Users select new {{ {string.Join(", ", fields)} }}";
        }

        private class User
        {
            public string Name { get; set; }

            public List<string> Tags { get; set; }
        }
    }
}
