using System;
using System.Collections.Generic;
using System.Reactive;
using System.Reactive.Linq;
using ActiproSoftware.Text;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Outlining;

namespace Raven.Studio.Extensions
{
    public static class SyntaxEditorExtensions
    {
        public static IObservable<EventPattern<TextSnapshotChangedEventArgs>> ObserveTextChanged(this ITextDocument document)
        {
            return Observable.FromEventPattern<TextSnapshotChangedEventArgs>(h => document.TextChanged += h,
                                                                             h => document.TextChanged -= h);
        } 

        public static void EnsureCollapsed(this IOutliningManager outliningManager)
        {
            outliningManager.ToggleAllOutliningExpansion();

            if ((outliningManager.GetOutliningState(outliningManager.Document.CurrentSnapshot.SnapshotRange) & OutliningState.HasExpandedNodeStart) 
                == OutliningState.HasExpandedNodeStart)
            {
                outliningManager.ToggleAllOutliningExpansion();
            }
        }

        public static IEnumerable<string> GetTextOfAllTokensMatchingType(this IEditorDocument document, string tokenType)
        {
            var reader = document.CurrentSnapshot.GetReader(0);

            while (!reader.IsAtSnapshotEnd)
            {
                var token = reader.PeekToken();

                if (token.Key == tokenType)
                {
                    yield return reader.ReadText(token.Length);
                }
                else
                {
                    reader.ReadToken();
                }
            }
        }
    }
}