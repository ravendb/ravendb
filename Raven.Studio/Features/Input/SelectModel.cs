using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.Studio.Features.Input
{
	public class SelectModel : InputModel
	{
		private readonly Func<Task<IList<string>>> provideList;

		public SelectModel(Func<Task<IList<string>>> provideList)
		{
			this.provideList = provideList;
		}

		public IList<string> Items
		{
			get { return provideList.Invoke().ContinueWith(task => task.Result).Result; }
		}
	}
}