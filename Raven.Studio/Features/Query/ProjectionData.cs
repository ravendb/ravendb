using System.Collections.Generic;
using Raven.Studio.Features.Documents;

namespace Raven.Studio.Features.Query
{
	public static class ProjectionData
	{
		private static Dictionary<string, ViewableDocument> projections;
		public static Dictionary<string, ViewableDocument> Projections
		{
			get { return projections ?? (projections = new Dictionary<string, ViewableDocument>()); }
			set { projections = value; }
		}
	}
}