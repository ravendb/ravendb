// -----------------------------------------------------------------------
//  <copyright file="DuoTreeTheory.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Runtime.Remoting.Messaging;
using Voron.Util;
using Xunit;
using Xunit.Sdk;

namespace Voron.Tests
{
    public class PrefixesFactAttribute : FactAttribute
    {
        protected override IEnumerable<ITestCommand> EnumerateTestCommands(IMethodInfo method)
        {
            yield return new PrefixedTreesFactCommand(method, "[Standard trees]");

            using (TreesWithPrefixedKeys())
            {
                yield return new PrefixedTreesFactCommand(method, "[Prefixed trees]");
            }		
        }

        internal static IDisposable TreesWithPrefixedKeys()
        {
            CallContext.LogicalSetData("Voron/Trees/KeysPrefixing", true);

            return new DisposableAction(() => CallContext.FreeNamedDataSlot("Voron/Trees/KeysPrefixing"));
        }
    }

    public class PrefixedTreesFactCommand : FactCommand
    {
        public PrefixedTreesFactCommand(IMethodInfo method, string testCaseDisplayName) : base(method)
        {
            DisplayName = string.Format("{0} {1}", method.Name, testCaseDisplayName);
        }
    }
}
