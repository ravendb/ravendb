using System;
using System.Net;
using System.Reactive;
using System.Reactive.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using ActiproSoftware.Text;

namespace Raven.Studio.Extensions
{
    public static class SyntaxEditorExtensions
    {
        public static IObservable<EventPattern<TextSnapshotChangedEventArgs>> ObserveTextChanged(this ITextDocument document)
        {
            return Observable.FromEventPattern<TextSnapshotChangedEventArgs>(h => document.TextChanged += h,
                                                                             h => document.TextChanged -= h);
        } 
    }
}
