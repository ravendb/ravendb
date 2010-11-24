// (c) Copyright Microsoft Corporation.
// This source is subject to the Microsoft Public License (Ms-PL).
// Please see http://go.microsoft.com/fwlink/?LinkID=131993 for details.
// All other rights reserved.

using System;
using System.Windows;
using System.Windows.Controls;

namespace Raven.ManagementStudio.UI.Silverlight.Controls.JeffWilcox.SyntaxHighlighting
{
    /// <summary>
    /// A simple text control for displaying syntax highlighted source code.
    /// </summary>
    [TemplatePart(Name = TextBlockName, Type = typeof(TextBlock))]
    public class SyntaxHighlightingTextBlock : Control
    {
        /// <summary>
        /// Shared static color coding system instance.
        /// </summary>
        private static WeakReference _colorizer;

        /// <summary>
        /// The name of the text block part.
        /// </summary>
        private const string TextBlockName = "TextBlock";

        /// <summary>
        /// Backing field for the text block.
        /// </summary>
        private TextBlock _textBlock;

        #region public SourceLanguageType SourceLanguage
        /// <summary>
        /// Gets or sets the source language type.
        /// </summary>
        public SourceLanguageType SourceLanguage
        {
            get { return (SourceLanguageType)GetValue(SourceLanguageProperty); }
            set { SetValue(SourceLanguageProperty, value); }
        }

        /// <summary>
        /// Identifies the SourceLanguage dependency property.
        /// </summary>
        public static readonly DependencyProperty SourceLanguageProperty =
            DependencyProperty.Register(
                "SourceLanguage",
                typeof(SourceLanguageType),
                typeof(SyntaxHighlightingTextBlock),
                new PropertyMetadata(SourceLanguageType.Json, OnSourceLanguagePropertyChanged));

        /// <summary>
        /// SourceLanguageProperty property changed handler.
        /// </summary>
        /// <param name="d">SyntaxHighlightingTextBlock that changed its SourceLanguage.</param>
        /// <param name="e">Event arguments.</param>
        private static void OnSourceLanguagePropertyChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            SyntaxHighlightingTextBlock source = d as SyntaxHighlightingTextBlock;
            SourceLanguageType value = (SourceLanguageType)e.NewValue;
            if (value != SourceLanguageType.Cpp ||
                value != SourceLanguageType.CSharp ||
                value != SourceLanguageType.JavaScript ||
                value != SourceLanguageType.VisualBasic ||
                value != SourceLanguageType.Xaml ||
                value != SourceLanguageType.Xml)
            {
                d.SetValue(e.Property, e.OldValue);
                throw new ArgumentException("Invalid source language type.");
            }

            if (e.NewValue != e.OldValue)
            {
                source.HighlightContents();
            }
        }
        #endregion public SourceLanguageType SourceLanguage

        #region public string SourceCode
        /// <summary>
        /// Gets or sets the source code to display inside the syntax
        /// highlighting text block.
        /// </summary>
        public string SourceCode
        {
            get { return GetValue(SourceCodeProperty) as string; }
            set { SetValue(SourceCodeProperty, value); }
        }

        /// <summary>
        /// Identifies the SourceCode dependency property.
        /// </summary>
        public static readonly DependencyProperty SourceCodeProperty =
            DependencyProperty.Register(
                "SourceCode",
                typeof(string),
                typeof(SyntaxHighlightingTextBlock),
                new PropertyMetadata(string.Empty, OnSourceCodePropertyChanged));

        /// <summary>
        /// SourceCodeProperty property changed handler.
        /// </summary>
        /// <param name="d">SyntaxHighlightingTextBlock that changed its SourceCode.</param>
        /// <param name="e">Event arguments.</param>
        private static void OnSourceCodePropertyChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            SyntaxHighlightingTextBlock source = d as SyntaxHighlightingTextBlock;
            source.HighlightContents();
        }
        #endregion public string SourceCode

        /// <summary>
        /// Initializes a new instance of the SyntaxHighlightingTextBlock
        /// control.
        /// </summary>
        public SyntaxHighlightingTextBlock()
        {
            DefaultStyleKey = typeof(SyntaxHighlightingTextBlock);
        }

        /// <summary>
        /// Overrides the on apply template method.
        /// </summary>
        public override void OnApplyTemplate()
        {
            base.OnApplyTemplate();

            _textBlock = GetTemplateChild(TextBlockName) as TextBlock;
            if (_textBlock != null && !string.IsNullOrEmpty(SourceCode))
            {
                HighlightContents();
            }
        }

        /// <summary>
        /// Clears and updates the contents.
        /// </summary>
        private void HighlightContents()
        {
            if (_textBlock != null)
            {
                _textBlock.Inlines.Clear();
                XamlInlineFormatter xif = new XamlInlineFormatter(_textBlock);

                CodeColorizer cc;
                if (_colorizer != null && _colorizer.IsAlive)
                {
                    cc = (CodeColorizer)_colorizer.Target;
                }
                else
                {
                    cc = new CodeColorizer();
                    _colorizer = new WeakReference(cc);
                }

                ILanguage language = CreateLanguageInstance(SourceLanguage);

                cc.Colorize(SourceCode, language, xif, new DefaultStyleSheet());
            }
        }

        /// <summary>
        /// Retrieves the language instance used by the highlighting system.
        /// </summary>
        /// <param name="type">The language type to create.</param>
        /// <returns>Returns a new instance of the language parser.</returns>
        private ILanguage CreateLanguageInstance(SourceLanguageType type)
        {
            switch (type)
            {
                case SourceLanguageType.CSharp:
                    return Languages.CSharp;
                    
                case SourceLanguageType.Cpp:
                    return Languages.Cpp;

                case SourceLanguageType.JavaScript:
                    return Languages.JavaScript;

                case SourceLanguageType.VisualBasic:
                    return Languages.VbDotNet;

                case SourceLanguageType.Xaml:
                case SourceLanguageType.Xml:
                    return Languages.Xml;
                case SourceLanguageType.Json:
                    return Languages.Json;

                default:
                    throw new InvalidOperationException("Could not locate the provider.");
            }
        }
    }
}