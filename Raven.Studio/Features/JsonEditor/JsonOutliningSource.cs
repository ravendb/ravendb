using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using ActiproSoftware.Text;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Outlining;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Outlining.Implementation;

namespace Raven.Studio.Features.JsonEditor
{
    public class JsonOutliningSource : TokenOutliningSourceBase
    {
        private static OutliningNodeDefinition squareBraceDefinition;
        private static OutliningNodeDefinition curlyBraceDefinition;

        public JsonOutliningSource(ITextSnapshot snapshot) : base(snapshot)
        {
        }

        static JsonOutliningSource()
        {
            squareBraceDefinition = new OutliningNodeDefinition("SquareBrace")
                                        {
                                            IsImplementation = true
                                        };

            curlyBraceDefinition = new OutliningNodeDefinition("CurlyBrace")
                                       {
                                           IsImplementation = true
                                       };

        }
        protected override ActiproSoftware.Windows.Controls.SyntaxEditor.Outlining.OutliningNodeAction GetNodeActionForToken(ActiproSoftware.Text.Lexing.IToken token, out ActiproSoftware.Windows.Controls.SyntaxEditor.Outlining.IOutliningNodeDefinition definition)
        {
            switch (token.Key)
            {
                case "OpenCurlyBrace":
                    definition = curlyBraceDefinition;
                    return OutliningNodeAction.Start;
                case "CloseCurlyBrace":
                    definition = curlyBraceDefinition;
                    return OutliningNodeAction.End;
                case "OpenSquareBrace":
                    definition = squareBraceDefinition;
                    return OutliningNodeAction.Start;
                case "CloseSquareBrace":
                    definition = squareBraceDefinition;
                    return OutliningNodeAction.End;
                default:
                    definition = null;
                    return OutliningNodeAction.None;
            }
        }
    }
}
