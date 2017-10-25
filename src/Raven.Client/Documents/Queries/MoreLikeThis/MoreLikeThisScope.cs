using System;
using Raven.Client.Documents.Session.Tokens;

namespace Raven.Client.Documents.Queries.MoreLikeThis
{
    public class MoreLikeThisScope : IDisposable
    {
        private readonly MoreLikeThisToken _token;
        private readonly Func<object, string> _addQueryParameter;
        private readonly Action _onDispose;

        public MoreLikeThisScope(MoreLikeThisToken token, Func<object, string> addQueryParameter, Action onDispose)
        {
            _token = token;
            _addQueryParameter = addQueryParameter;
            _onDispose = onDispose;
        }

        public void Dispose()
        {
            _onDispose?.Invoke();
        }

        public void WithOptions(MoreLikeThisOptions options)
        {
            if (options == null)
                return;

            _token.OptionsParameterName = _addQueryParameter(options);
        }

        public void WithDocument(string document)
        {
            _token.DocumentParameterName = _addQueryParameter(document);
        }
    }
}
