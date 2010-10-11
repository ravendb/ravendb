using System;

namespace Raven.Bundles.Authorization.Model
{
	public class OperationPermission : IPermission
	{
		public string Operation { get; set; }
		public string Tag { get; set; }
		public bool Allow { get; set; }
		public int Priority { get; set; }

		public string Explain
		{
			get
			{
				return string.Format("Operation: {0}, Tag: {1}, Allow: {2}, Priority: {3}", Operation, Tag, Allow, Priority);
			}
		}
	}
}
