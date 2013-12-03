using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Raven.Client.UniqueConstraints
{
	public class ReflectedConstraintInfo : ConstraintInfo
	{
		public ReflectedConstraintInfo(PropertyInfo property, UniqueConstraintAttribute attr) : base()
		{
			if (property == null) { throw new ArgumentNullException("property"); }

			this.Property = property;
			this.Configuration.Name = property.Name;

			if (attr != null)
			{
				this.Configuration.CaseInsensitive = attr.CaseInsensitive;
			}
		}

		public PropertyInfo Property { get; private set; }

		public override object GetValue(object entity)
		{
			object value = this.Property.GetValue(entity, null);

			if (value == null) { return null; }

			return value;
		}
	}
}
