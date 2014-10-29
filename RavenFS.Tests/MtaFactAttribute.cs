// //-----------------------------------------------------------------------
// // <copyright company="Hibernating Rhinos LTD">
// //     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// // </copyright>
// //-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Sdk;

namespace RavenFS.Tests
{
	public class MtaFactAttribute : FactAttribute
	{
		protected override IEnumerable<ITestCommand> EnumerateTestCommands(IMethodInfo method)
		{
			return base.EnumerateTestCommands(method)
				.Select(x => new MTAThreadTimeoutCommand(x, method));
		}

		public class MTAThreadTimeoutCommand : DelegatingTestCommand
		{
			private readonly IMethodInfo _testMethod;

			public MTAThreadTimeoutCommand(ITestCommand innerComand, IMethodInfo testMethod)
				: base(innerComand)
			{
				_testMethod = testMethod;
			}

			public override MethodResult Execute(object testClass)
			{
				var task = Task.Factory.StartNew(() =>
				{
					try
					{
						using (testClass as IDisposable)
							return InnerCommand.Execute(testClass);
					}
					catch (Exception ex)
					{
						return new FailedResult(_testMethod, ex, DisplayName);
					}
				});

				return task.Result;
			}
		}
	}
}