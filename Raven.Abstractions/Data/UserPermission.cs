using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.AccessControl;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.Data
{
	public class UserPermission
	{
		public string User { get; set; }
		public DatabaseInfo Database{ get; set; }
		public string Method { get; set; }
		public bool IsGranted { get; set; }
		public string Reason { get; set; }
	}
}
