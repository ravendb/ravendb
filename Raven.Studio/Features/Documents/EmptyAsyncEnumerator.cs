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
using Raven.Abstractions.Util;

namespace Raven.Studio.Features.Documents
{
    public class EmptyAsyncEnumerator<TResult> : IAsyncEnumerator<TResult>
    {
        public EmptyAsyncEnumerator()
        {
        }

        public void Dispose()
        {
        }

        public Task<bool> MoveNextAsync()
        {
            return TaskEx.FromResult(false);
        }

        public TResult Current { get { throw new InvalidOperationException();} }
    }
}
