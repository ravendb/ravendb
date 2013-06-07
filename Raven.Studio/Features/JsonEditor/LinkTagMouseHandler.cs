using System;
using System.Linq;
using System.Windows;
using System.Windows.Input;
using ActiproSoftware.Compatibility;
using ActiproSoftware.Text;
using ActiproSoftware.Windows.Controls.SyntaxEditor;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt;
using Raven.Studio.Extensions;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.JsonEditor
{
    public class LinkTagMouseHandler : IEditorViewMouseInputEventSink
    {
        private LinkTagQuickInfoProvider tagInfoProvider;

        public LinkTagMouseHandler()
        {
            tagInfoProvider = new LinkTagQuickInfoProvider();
        }

        public void NotifyMouseDown(IEditorView view, MouseButtonEventArgs e)
        {
            if ((Keyboard.Modifiers & ModifierKeys.Shift) != ModifierKeys.Shift)
                return;

            var hitTestResult = view.SyntaxEditor.HitTest(e.GetPosition(view.SyntaxEditor));
            if (hitTestResult.Type != HitTestResultType.ViewTextAreaOverCharacter)
                return;

            var tag = GetLinkTag(view, hitTestResult.Offset);
            if (tag == null)
                return;

            if (tag.NavigationType == LinkTagNavigationType.ExternalUrl)
                UrlUtil.NavigateToExternal(tag.Url);
            else
                UrlUtil.Navigate("/Edit?id=" + tag.Url);

            e.Handled = true;
        }

        public LinkTag GetLinkTag(IEditorView view, int offset)
        {
            using (var tagAggregator = view.CreateTagAggregator<LinkTag>())
            {
                var tagRange = tagAggregator.GetTags(new[] { new TextSnapshotRange(view.CurrentSnapshot, offset) })
                    .FirstOrDefault(tag => tag.Tag != null && tag.SnapshotRange.Contains(offset));

                return tagRange != null ? tagRange.Tag : null;
            }
        }

        public void NotifyMouseEnter(IEditorView view, MouseEventArgs e)
        {

        }

        public void NotifyMouseHover(IEditorView view, RoutedEventArgsEx e)
        {
            var state = GetState(view);

            var result = view.SyntaxEditor.HitTest(state.LastMousePosition);
            var context = tagInfoProvider.GetContext(result) as LinkTagContext;

            if (state.Session == null && context != null)
            {
                tagInfoProvider.RequestSession(view, context, canTrackMouse: false);

                state.Session =
                    view.SyntaxEditor.IntelliPrompt.Sessions.OfType<IQuickInfoSession>()
                        .FirstOrDefault(s => s.Context == context);

                if (state.Session != null)
                {
                    state.Session.Closed += delegate { state.Session = null; };
                }

            }
        }

        public void NotifyMouseLeave(IEditorView view, MouseEventArgs e)
        {

        }

        public void NotifyMouseMove(IEditorView view, MouseEventArgs e)
        {
            var state = GetState(view);

            state.LastMousePosition = e.GetPosition(view.SyntaxEditor);

            if (state.Session != null)
            {
                try
                {
                    var inflatedBounds = state.Session.Bounds.Value.Inflate(40);
                    if (!inflatedBounds.Contains(state.LastMousePosition))
                    {
                        state.Session.Close(true);
                    }
                }
                catch (ArgumentException)
                {
                    
                }
            }
        }

        public void NotifyMouseUp(IEditorView view, MouseButtonEventArgs e)
        {

        }

        public void NotifyMouseWheel(IEditorView view, MouseWheelEventArgs e)
        {

        }

        private HandlerViewState GetState(IEditorView view)
        {
            return view.Properties.GetOrCreateSingleton(() => new HandlerViewState());
        }

        private class HandlerViewState
        {
            public Point LastMousePosition;
            public IQuickInfoSession Session;
        }
    }
}