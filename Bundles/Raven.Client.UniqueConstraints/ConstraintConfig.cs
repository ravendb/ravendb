using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Client.UniqueConstraints
{
	public class ConstraintConfig
	{
		public string Name { get; set; }
		public bool CaseInsensitive { get; set; }
	}
}
