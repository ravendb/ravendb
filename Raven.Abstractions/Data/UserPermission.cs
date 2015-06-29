using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.Data
{
	public class UserPermission
	{
		public bool IsGranted { get; set; }
		public string Reason { get; set; }
	}
}
