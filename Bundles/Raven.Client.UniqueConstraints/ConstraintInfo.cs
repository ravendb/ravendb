using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Raven.Client.UniqueConstraints
{
	public abstract class ConstraintInfo
	{
		public ConstraintInfo()
		{
			this.Configuration = new ConstraintConfig();
		}

		public ConstraintConfig Configuration { get; set; }

		public abstract object GetValue(object entity);
	}
}
