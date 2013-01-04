using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.Studio.Behaviors
{
    public interface IAutoCompleteSuggestionProvider
    {
        Task<IList<object>> ProvideSuggestions(string enteredText);
    }
}