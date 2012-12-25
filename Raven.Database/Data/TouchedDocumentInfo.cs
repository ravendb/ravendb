using System;
using System.Collections.Generic;

namespace Raven.Database.Data
{
	public class TouchedDocumentInfo
	{
		public Guid TouchedEtag { get; set; }
		public Guid PreTouchEtag { get; set; }
	}
}