using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Studio.Behaviors;

namespace Raven.Studio.Features.Input
{
	public class InputModelWithSuggetion : InputModel, IAutoCompleteSuggestionProvider
	{
		private readonly Task<IList<object>> provideSuggestions;

		public InputModelWithSuggetion(Task<IList<object>> provideSuggestions)
		{
			this.provideSuggestions = provideSuggestions;
		}

		public Task<IList<object>> ProvideSuggestions(string enteredText)
		{
			return provideSuggestions;
		}
	}
}
