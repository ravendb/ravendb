// -----------------------------------------------------------------------
//  <copyright file="TimeBombedFactAttribute.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml;
using Xunit;
using Xunit.Sdk;

namespace Raven.Tests.Common.Attributes
{
	public class TimeBombedFactAttribute : FactAttribute, ITestCommand
	{
		public TimeBombedFactAttribute(int year, int month, int day, string msg)
		{
			SkipUntil = new DateTime(year, month, day);
		}

		public DateTime SkipUntil { get; set; }

		protected override IEnumerable<ITestCommand> EnumerateTestCommands(IMethodInfo method)
		{
			if (DateTime.Today < SkipUntil)
				return Enumerable.Empty<ITestCommand>();
		    DisplayName = method.TypeName + "." + method.Name;
		    return new[] {this};
		    //return base.EnumerateTestCommands(method);
		}


	    public MethodResult Execute(object testClass)
	    {
            throw new InvalidOperationException("Time bombed fact expired");
        }

        public XmlNode ToStartXml()
        {
            XmlDocument doc = new XmlDocument();
            doc.LoadXml("<dummy/>");
            XmlNode testNode = XmlUtility.AddElement(doc.ChildNodes[0], "start");

            XmlUtility.AddAttribute(testNode, "name", DisplayName);

            return testNode;
        }

	    public bool ShouldCreateInstance { get { return false; } }
	}
}