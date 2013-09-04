using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Connection.Async;
using Raven.Studio.Models;

namespace Raven.Studio.Infrastructure
{
	public abstract class PageViewModel : ViewModel
	{
		public List<string> ModelUrlIgnoreList { get; private set; }
		public string ModelUrl { get; set; }

		public PageViewModel()
		{
			ModelUrlIgnoreList = new List<string>();
		}

		public void LoadModel(string state)
		{
			if (string.IsNullOrWhiteSpace(state) == false &&
				state.StartsWith(ModelUrl, StringComparison.OrdinalIgnoreCase) &&
				ModelUrlIgnoreList.Any(state.StartsWith) == false)
			{
				LoadModelParameters(state.Substring(ModelUrl.Length));
				ForceTimerTicked();
			}
		}

		public virtual void LoadModelParameters(string parameters) { }

		public override Task TimerTickedAsync()
		{
			return IsLoaded ? LoadedTimerTickedAsync() : null;
		}

		protected virtual Task LoadedTimerTickedAsync()
		{
			return null;
		}

	    public virtual bool CanLeavePage()
		{
			return true;
		}
	}
}