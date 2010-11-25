// (c) Copyright Microsoft Corporation.
// This source is subject to the Microsoft Public License (Ms-PL).
// Please see http://go.microsoft.com/fwlink/?LinkID=131993 for details.
// All other rights reserved.


using System;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;

namespace Raven.ManagementStudio.UI.Silverlight.Controls.JeffWilcox.SyntaxHighlighting
{
    /// <summary>
    /// A simple text control for displaying syntax highlighted source code.
    /// </summary>
    [TemplatePart(Name = TextBlockName, Type = typeof(TextBlock))]
    public class SyntaxHighlightingTextBox : Control
    {
        /// <summary>
        /// Shared static color coding system instance.
        /// </summary>
        private static WeakReference _colorizer;

        /// <summary>
        /// The name of the text block part.
        /// </summary>
        private const string TextBlockName = "TextBlock";
		private const string TextBlockSelectionName = "TextBlockSelection";

        /// <summary>
        /// Backing field for the text block.
        /// </summary>
        private TextBlock _textBlock;
		private TextBox _textBlockSelection;

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
                typeof(SyntaxHighlightingTextBox),
                new PropertyMetadata(SourceLanguageType.CSharp, OnSourceLanguagePropertyChanged));

        /// <summary>
        /// SourceLanguageProperty property changed handler.
        /// </summary>
        /// <param name="d">SyntaxHighlightingTextBlock that changed its SourceLanguage.</param>
        /// <param name="e">Event arguments.</param>
        private static void OnSourceLanguagePropertyChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            SyntaxHighlightingTextBox source = d as SyntaxHighlightingTextBox;
            source.HighlightContents();
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
                typeof(SyntaxHighlightingTextBox),
                new PropertyMetadata(string.Empty, OnSourceCodePropertyChanged));

        /// <summary>
        /// SourceCodeProperty property changed handler.
        /// </summary>
        /// <param name="d">SyntaxHighlightingTextBlock that changed its SourceCode.</param>
        /// <param name="e">Event arguments.</param>
        private static void OnSourceCodePropertyChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            SyntaxHighlightingTextBox source = d as SyntaxHighlightingTextBox;
            source.HighlightContents();
        }
        #endregion public string SourceCode

        /// <summary>
        /// Initializes a new instance of the SyntaxHighlightingTextBlock
        /// control.
        /// </summary>
        public SyntaxHighlightingTextBox()
        {
            DefaultStyleKey = typeof(SyntaxHighlightingTextBox);
        }

        /// <summary>
        /// Overrides the on apply template method.
        /// </summary>
        public override void OnApplyTemplate()
        {
            base.OnApplyTemplate();

            _textBlock = GetTemplateChild(TextBlockName) as TextBlock;
			_textBlockSelection = GetTemplateChild(TextBlockSelectionName) as TextBox;
			if (_textBlock != null && !string.IsNullOrEmpty(SourceCode))
			{
				HighlightContents();
			}
			if (_textBlockSelection != null)
			{
				_textBlockSelection.TextChanged += _textBlockSelection_TextChanged;
			}
        }

		private void _textBlockSelection_TextChanged(object sender, TextChangedEventArgs e)
		{
			this.SourceCode = (sender as TextBox).Text;
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

		/// <summary>
		/// Gets or sets the content of the current selection in the text box.
		/// </summary>
		/// <value>The currently selected text in the text box. If no text is selected, the
		///  value is System.String.Empty.</value>
		public string SelectedText
		{
			get
			{
				if (_textBlockSelection != null)
					return _textBlockSelection.SelectedText;
				return String.Empty;
			}
			set
			{
				if (_textBlockSelection != null)
					_textBlockSelection.SelectedText = value;
			}
		}

		/// <summary>
		/// Gets or sets the brush that fills the background of the selected text.
		/// </summary>
		/// <value>The brush that fills the background of the selected text.</value>
		public Brush SelectionBackground
		{
			get { return (Brush)GetValue(SelectionBackgroundProperty); }
			set { SetValue(SelectionBackgroundProperty, value); }
		}

		/// <summary>
		/// Identifies the <see cref="SelectionBackground"/> dependency property.
		/// </summary>
		public static readonly DependencyProperty SelectionBackgroundProperty =
			DependencyProperty.Register("SelectionBackground", typeof(Brush), typeof(SyntaxHighlightingTextBox), null);

		/// <summary>
		/// Gets or sets the brush used for the selected text in the text box.
		/// </summary>
		/// <value>The brush used for the selected text in the text box.</value>
		public Brush SelectionForeground
		{
			get { return (Brush)GetValue(SelectionForegroundProperty); }
			set { SetValue(SelectionForegroundProperty, value); }
		}

		/// <summary>
		/// Identifies the <see cref="SelectionForeground"/> dependency property.
		/// </summary>
		public static readonly DependencyProperty SelectionForegroundProperty =
			DependencyProperty.Register("SelectionForeground", typeof(Brush), typeof(SyntaxHighlightingTextBox), null);

		/// <summary>
		/// Gets or sets the number of characters in the current selection in the text box.
		/// </summary>
		/// <value>The number of characters in the current selection in the text box, or 0 if there is no selection.</value>
		public int SelectionLength
		{
			get
			{
				if (_textBlockSelection != null)
					return _textBlockSelection.SelectionLength;
				else return 0;
			}
			set
			{
				if (_textBlockSelection != null)
					_textBlockSelection.SelectionLength = value;
			}
		}

		/// <summary>
		/// Gets or sets the starting position of the text selected in the text box.
		/// </summary>
		/// <value>The starting position of the current selection.</value>
		public int SelectionStart
		{
			get
			{
				if (_textBlockSelection != null)
					return _textBlockSelection.SelectionStart;
				else return 0;
			}
			set
			{
				if (_textBlockSelection != null)
					_textBlockSelection.SelectionStart = value;
			}
		}



		/// <summary>
		/// Gets or sets the value that determines if the user can change the text in
		/// the text box.
		/// </summary>
		/// <value>
		/// 	<c>true</c> if this instance is read only; otherwise, <c>false</c>. The default is false.
		/// </value>
		public bool IsReadOnly
		{
			get { return (bool)GetValue(IsReadOnlyProperty); }
            set { SetValue(IsReadOnlyProperty, value); }
		}

		/// <summary>
		/// Identifies the <see cref="IsReadOnly"/> dependency property.
		/// </summary>
		public static readonly DependencyProperty IsReadOnlyProperty =
			DependencyProperty.Register("IsReadOnly", typeof(bool), typeof(SyntaxHighlightingTextBox), new PropertyMetadata(false));
    }
}