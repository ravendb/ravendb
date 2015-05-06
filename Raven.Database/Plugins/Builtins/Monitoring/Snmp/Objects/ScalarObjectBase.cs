// -----------------------------------------------------------------------
//  <copyright file="ScalarObjectBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects
{
	public abstract class ScalarObjectBase : ScalarObject
	{
		private const string RootOid = "1.3.6.1.4.1.45751.1.1.";

		protected static readonly Null Null = new Null();

		protected ScalarObjectBase(string dots)
			: base(RootOid + dots)
		{
		}

		protected ScalarObjectBase(string dots, int index)
			: base(RootOid + dots, index)
		{
		}
	}
}