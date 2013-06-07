using System;
using System.Net;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Document.Async;
using Raven.Json.Linq;

namespace Raven.Studio.Features.Documents
{
    public class ConvertingEnumerator<TResult, TInput> : IAsyncEnumerator<TResult>
    {
        private readonly IAsyncEnumerator<TInput> enumerator;
        private readonly Func<TInput, TResult> converter;

        public ConvertingEnumerator(IAsyncEnumerator<TInput> enumerator, Func<TInput, TResult> converter)
        {
            this.enumerator = enumerator;
            this.converter = converter;
        }

        public void Dispose()
        {
            enumerator.Dispose();
        }

        public Task<bool> MoveNextAsync()
        {
            return enumerator.MoveNextAsync();
        }

        public TResult Current { get { return converter(enumerator.Current); } }
    }
}
