// -----------------------------------------------------------------------
//  <copyright file="UintId.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Converters;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class UintId : RavenTest
	{
		public class UInt32Converter : ITypeConverter
		{
			public bool CanConvertFrom(Type sourceType)
			{
				return sourceType == typeof(uint);
			}
			public string ConvertFrom(string tag, object value, bool allowNull)
			{
				var val = (uint)value;
				if (val == 0 && allowNull)
					return null;
				return tag + value;
			}
			public object ConvertTo(string value)
			{
				return uint.Parse(value);
			}
		}

		public class Foo
		{
			public uint Id { get; set; }
			private List<uint> _related;
			public List<uint> Related
			{
				get { return _related ?? (Related = new List<uint>()); }
				set { _related = value; }
			}
		}
		public class Bar
		{
			public uint Id { get; set; }
		}


		[Fact]
		public void ShouldWork()
		{
			using (var store = NewDocumentStore())
			{
				store.Conventions.IdentityTypeConvertors.Add(new UInt32Converter());
				using (var session = store.OpenSession())
				{
					var foo = new Foo() { Id = uint.MaxValue };
					foo.Related.Add(uint.MaxValue);
					session.Store(foo);
					var bar = new Bar { Id = uint.MaxValue };
					session.Store(bar);
					session.SaveChanges();
				}
				using (var session = store.OpenSession())
				{
					var foo = session.Query<Foo>()
						.Customize(x=>x.WaitForNonStaleResults())
						.ToList();
					var bar = session.Query<Bar>().ToList();
					//This line blows up
					var foobar = session.Query<Foo>().Include<Foo, Bar>(f => f.Related).ToList();
				}
			}
		}
	}
}