using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Net;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Lexing;
using ActiproSoftware.Text.Utility;
using ActiproSoftware.Windows.Controls.SyntaxEditor;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt.Implementation;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Studio.Features.Documents;
using Raven.Studio.Models;
using Raven.Abstractions.Extensions;
using System.Linq;

namespace Raven.Studio.Features.Patch
{
    public class PatchScriptIntelliPromptProvider : ICompletionProvider
    {
        private readonly IList<PatchValue> patchValues;
        private readonly IList<JsonDocument> recentDocuments;
        private bool documentPropertyCacheUpToDate;
        private List<string> documentProperties = new List<string>();
 
        public PatchScriptIntelliPromptProvider(IList<PatchValue> patchValues, ObservableCollection<JsonDocument> recentDocuments)
        {
            this.patchValues = patchValues;
            this.recentDocuments = recentDocuments;
            recentDocuments.CollectionChanged += delegate { documentPropertyCacheUpToDate = false; };
        }

        public string Key { get; private set; }
        public IEnumerable<Ordering> Orderings { get; private set; }

        
        public bool RequestSession(IEditorView view, bool canCommitWithoutPopup)
        {
            var session = new CompletionSession()
            {

            };

            var completionContext = GetCompletionContext(view);

            if (completionContext.IsDocumentProperty)
            {
                var properties = GetProperties(completionContext.CompletedPropertyPath);
                foreach (var property in properties)
                {
                    session.Items.Add(new CompletionItem()
                    {
                        ImageSourceProvider = new CommonImageSourceProvider(CommonImage.PropertyPublic),
                        Text = property,
                        AutoCompletePreText = property,
                        DescriptionProvider =
                            new HtmlContentProvider(string.Format("Document Property <em>{0}</em>", property))
                    }); 
                }
            }
            else if (!completionContext.IsObjectMember)
            {
                session.Items.Add(new CompletionItem()
                {
                    ImageSourceProvider = new CommonImageSourceProvider(CommonImage.MethodPublic),
                    Text = "LoadDocument",
                    AutoCompletePreText = "LoadDocument",
                    DescriptionProvider =
                        new HtmlContentProvider("(<em>documentId</em>)<br/>Loads the document with the given id")
                });

                foreach (var patchValue in patchValues)
                {
                    session.Items.Add(new CompletionItem()
                    {
                        ImageSourceProvider = new CommonImageSourceProvider(CommonImage.ConstantPublic),
                        Text = patchValue.Key,
                        AutoCompletePreText = patchValue.Key,
                        DescriptionProvider =
                            new HtmlContentProvider(
                                          string.Format("Script Parameter <em>{0}</em>. Current value: <em>{1}</em>",
                                                        patchValue.Key, patchValue.Value))
                    });
                }
            }

            if (session.Items.Count > 0)
            {
                session.Open(view);
                return true;
            }
            else
            {
                return false;
            }
        }

        private IEnumerable<string> GetProperties(string completedPropertyPath)
        {
            var parsedPath = new RavenJPath(completedPropertyPath);

            var matchingProperties =
                recentDocuments.SelectMany(doc => GetPropertiesAtEndOfPath(doc, parsedPath)).Distinct();

            return matchingProperties;
        }

        private IEnumerable<string> GetPropertiesAtEndOfPath(JsonDocument document, RavenJPath path)
        {
            var currentObject = document.DataAsJson.SelectToken(path);

            if (currentObject is RavenJObject)
            {
                return (currentObject as RavenJObject).Keys;
            }
            else
            {
                return new string[0];
            }
        }

        private CompletionContext GetCompletionContext(IEditorView view)
        {
            var reader = view.GetReader();

            var token = reader.ReadTokenReverse();
            var completionContext = new CompletionContext();

            if (token == null)
            {
                return completionContext;
            }

            var tokenText = reader.PeekText(token.Length);
            if (token.Key == "Identifier" || (token.Key == "Punctuation" && tokenText == "."))
            {
                var memberExpression = DetermineFullMemberExpression(tokenText, reader);

                if (memberExpression.Contains("."))
                {
                    completionContext.IsObjectMember = true;
                }

                if (memberExpression.StartsWith("this."))
                {
                    completionContext.IsDocumentProperty = true;
                    var completedPath = memberExpression.Substring("this.".Length);
                    var lastDot = completedPath.LastIndexOf('.');
                    completedPath = lastDot >= 0 ? completedPath.Substring(0, lastDot) : "";
                    completionContext.CompletedPropertyPath = completedPath;
                }
            }

            return completionContext;
        }

        private string DetermineFullMemberExpression(string tokenText, ITextSnapshotReader reader)
        {
            var sb = new StringBuilder(tokenText);
            var token = reader.ReadTokenReverse();
            while (token != null)
            {
                var text = reader.PeekText(token.Length);

                if (token.Key == "Identifier" || (token.Key == "Punctuation" && text == ".") || (token.Key == "Keyword" && text == "this"))
                {
                    sb.Insert(0, text);
                }
                else if (token.Key == "CloseSquareBrace")
                {
                    var indexExpression = ReadArrayIndexExpression(reader);
                    if (indexExpression == null)
                    {
                        // we're not going to be able to make sense
                        // of the rest of the expression, so bail out.
                        break;
                    }

                    sb.Insert(0, indexExpression);
                }
                else if (token.Key == "Whitespace")
                {
                    // skip it
                }
                else
                {
                    break;
                }

                token = reader.ReadTokenReverse();
            }

            return sb.ToString();
        }

        private string ReadArrayIndexExpression(ITextSnapshotReader reader)
        {
            // we're looking for an expression of the form [123] or [myVariable]
            // if we don't find one, return false.
            string indexValue = null;

            var token = reader.ReadTokenReverse();
            while (token != null)
            {
                var text = reader.PeekText(token.Length);

                if (token.Key == "Identifier" && indexValue == null)
                {
                    // substitue 0 for the name of the variable to give us 
                    // the best chance of matching something when we look up the path in a document
                    indexValue = "0";
                }
                else if ((token.Key == "IntegerNumber") && indexValue == null)
                {
                    indexValue = text;
                }
                else if (token.Key == "Whitespace")
                {
                    // skip it
                }
                else if (token.Key == "OpenSquareBrace")
                {
                    if (indexValue == null)
                    {
                        // we didn't find a properly formed (and simple) index expression
                        // before hitting the square brace
                        return null;
                    }
                    else
                    {
                        break;
                    }
                }

                token = reader.ReadTokenReverse();
            }

            if (indexValue == null)
            {
                return null;
            }

            return "[" + indexValue + "]";
        }

        private class CompletionContext
        {
            public bool IsObjectMember;

            public bool IsDocumentProperty;

            public string CompletedPropertyPath = "";
        }
    }
}
