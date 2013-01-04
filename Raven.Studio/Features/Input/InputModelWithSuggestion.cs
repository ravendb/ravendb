using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Studio.Behaviors;

namespace Raven.Studio.Features.Input
{
	public class InputModelWithSuggestion : InputModel, IAutoCompleteSuggestionProvider
	{
		private readonly Func<string, Task<IList<object>>> provideSuggestions;

		public InputModelWithSuggestion(Func<string, Task<IList<object>>> provideSuggestions)
		{
			this.provideSuggestions = provideSuggestions;
		}

		public Task<IList<object>> ProvideSuggestions(string enteredText)
		{
			return provideSuggestions(enteredText);
		}
	}
}
