using System;
using System.Linq;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using ActiproSoftware.Compatibility;
using ActiproSoftware.Text;
using ActiproSoftware.Windows.Controls.SyntaxEditor;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.JsonEditor
{
    public class LinkTagClickHandler : IEditorViewMouseInputEventSink
    {
        public LinkTagClickHandler()
        {
        }

        public void NotifyMouseDown(IEditorView view, MouseButtonEventArgs e)
        {
            if ((Keyboard.Modifiers & ModifierKeys.Shift) != ModifierKeys.Shift)
            {
                return;
            }

            var hitTestResult = view.SyntaxEditor.HitTest(e.GetPosition(view.SyntaxEditor));
            if (hitTestResult.Type != HitTestResultType.ViewTextAreaOverCharacter)
            {
                return;
            }

            var tag = GetLinkTag(view, hitTestResult.Offset);
            if (tag == null)
            {
                return;
            }

            if (tag.NavigationType == LinkTagNavigationType.ExternalUrl)
            {
                UrlUtil.NavigateToExternal(tag.Url);
            }
            else
            {
                UrlUtil.Navigate("/Edit?id=" + tag.Url);
            }

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

        }

        public void NotifyMouseLeave(IEditorView view, MouseEventArgs e)
        {

        }

        public void NotifyMouseMove(IEditorView view, MouseEventArgs e)
        {

        }

        public void NotifyMouseUp(IEditorView view, MouseButtonEventArgs e)
        {

        }

        public void NotifyMouseWheel(IEditorView view, MouseWheelEventArgs e)
        {

        }
    }
}
