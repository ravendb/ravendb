// -----------------------------------------------------------------------
//  <copyright file="PrefixesTheory.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Xunit.Extensions;
using Xunit.Sdk;

namespace Voron.Tests
{
    public class PrefixesTheoryAttribute : TheoryAttribute
    {
        protected override IEnumerable<ITestCommand> EnumerateTestCommands(IMethodInfo method)
        {
            foreach (var command in base.EnumerateTestCommands(method))
            {
                yield return new PrefixedTreesTheoryCommand((TheoryCommand) command, method, "[Standard trees]");
            }

            using (PrefixesFactAttribute.TreesWithPrefixedKeys())
            {
                foreach (var command in base.EnumerateTestCommands(method))
                {
                    yield return new PrefixedTreesTheoryCommand((TheoryCommand)command, method, "[Prefixed trees]");
                }
            }
        }
    }

    public class PrefixedTreesTheoryCommand : TestCommand
    {
        private readonly TheoryCommand command;

        public PrefixedTreesTheoryCommand(TheoryCommand command, IMethodInfo method, string testCaseDisplayName)
            : base(method, string.Format("{0} {1}", command.DisplayName, testCaseDisplayName), command.Timeout)
        {
            this.command = command;
        }

        public override MethodResult Execute(object testClass)
        {
            return command.Execute(testClass);
        }
    }
}
