// -----------------------------------------------------------------------
//  <copyright file="ScalarObjectBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects
{
	public abstract class ScalarObjectBase<TData> : ScalarObject
		where TData : ISnmpData
	{
		private const string RootOid = "1.3.6.1.4.1.45751.1.1.";

		protected ScalarObjectBase(string dots)
			: base(RootOid + dots)
		{
		}

		protected ScalarObjectBase(string dots, int index)
			: base(RootOid + dots, index)
		{
		}

		protected abstract TData GetData();

		public override ISnmpData Data
		{
			get
			{
				var data = GetData();
				if (data == null)
					return DefaultValue();

				return data;
			}

			set
			{
				throw new AccessFailureException();
			}
		}

		protected ISnmpData DefaultValue()
		{
			var type = typeof(TData);
			if (type == typeof(OctetString)) 
				return DefaultOctetString;

			if (type == typeof(Integer32))
				return DefaultInteger32;

			if (type == typeof(Gauge32))
				return DefaultGauge32;

			if (type == typeof(TimeTicks))
				return DefaultTimeTicks;

			throw new NotSupportedException(type.ToString());
		}

		private static TimeTicks DefaultTimeTicks = new TimeTicks(0);

		private static Gauge32 DefaultGauge32 = new Gauge32(0);

		private static Integer32 DefaultInteger32 = new Integer32(0);

		private static OctetString DefaultOctetString = new OctetString("N/A");
	}
}