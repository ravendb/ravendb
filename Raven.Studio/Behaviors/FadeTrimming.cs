using System;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Data;
using System.Windows.Media;

namespace Raven.Studio.Behaviors
{
    public static class FadeTrimming
    {
        const double Epsilon = 0.00001;
        private const double FadeWidth = 10.0;
        private const double FadeHeight = 20.0;

        public static readonly DependencyProperty IsEnabledProperty =
            DependencyProperty.RegisterAttached("IsEnabled", typeof(bool), typeof(FadeTrimming), new PropertyMetadata(false, HandleIsEnabledChanged));

        public static readonly DependencyProperty ForegroundColorProperty =
            DependencyProperty.RegisterAttached("ForegroundColor", typeof(Color), typeof(FadeTrimming), new PropertyMetadata(Colors.Transparent));

        public static readonly DependencyProperty ShowTextInToolTipWhenTrimmedProperty =
            DependencyProperty.RegisterAttached("ShowTextInToolTipWhenTrimmed", typeof(bool), typeof(FadeTrimming), new PropertyMetadata(false));

        private static readonly DependencyProperty FaderProperty =
            DependencyProperty.RegisterAttached("Fader", typeof(Fader), typeof(FadeTrimming), new PropertyMetadata(null));

        public static readonly DependencyProperty ToolTipStyleProperty =
            DependencyProperty.RegisterAttached("ToolTipStyle", typeof(Style), typeof(FadeTrimming), new PropertyMetadata(null));

        public static Style GetToolTipStyle(DependencyObject obj)
        {
            return (Style)obj.GetValue(ToolTipStyleProperty);
        }

        public static void SetToolTipStyle(DependencyObject obj, Style value)
        {
            obj.SetValue(ToolTipStyleProperty, value);
        }

        public static bool GetIsEnabled(DependencyObject obj)
        {
            return (bool)obj.GetValue(IsEnabledProperty);
        }

        public static void SetIsEnabled(DependencyObject obj, bool value)
        {
            obj.SetValue(IsEnabledProperty, value);
        }

        public static bool GetShowTextInToolTipWhenTrimmed(DependencyObject obj)
        {
            return (bool)obj.GetValue(ShowTextInToolTipWhenTrimmedProperty);
        }

        public static void SetShowTextInToolTipWhenTrimmed(DependencyObject obj, bool value)
        {
            obj.SetValue(ShowTextInToolTipWhenTrimmedProperty, value);
        }

        public static Color GetForegroundColor(DependencyObject obj)
        {
            return (Color)obj.GetValue(ForegroundColorProperty);
        }

        public static void SetForegroundColor(DependencyObject obj, Color value)
        {
            obj.SetValue(ForegroundColorProperty, value);
        }

        private static Fader GetFader(DependencyObject obj)
        {
            return (Fader)obj.GetValue(FaderProperty);
        }

        private static void SetFader(DependencyObject obj, Fader value)
        {
            obj.SetValue(FaderProperty, value);
        }

        private static void HandleIsEnabledChanged(DependencyObject source, DependencyPropertyChangedEventArgs e)
        {
            var textBlock = source as TextBlock;
            if (textBlock == null)
            {
                return;
            }

            if ((bool)e.OldValue)
            {
                var fader = GetFader(textBlock);
                if (fader != null)
                {
                    fader.Detach();
                    SetFader(textBlock, null);
                }

                textBlock.Loaded -= HandleTextBlockLoaded;
                textBlock.Unloaded -= HandleTextBlockUnloaded;
            }

            if ((bool)e.NewValue)
            {
                textBlock.Loaded += HandleTextBlockLoaded;
                textBlock.Unloaded += HandleTextBlockUnloaded;

                var fader = new Fader(textBlock);
                SetFader(textBlock, fader);
                fader.Attach();
            }
        }

        private static void HandleTextBlockUnloaded(object sender, RoutedEventArgs e)
        {
            var fader = GetFader(sender as DependencyObject);
            fader.Detach();
        }

        private static void HandleTextBlockLoaded(object sender, RoutedEventArgs e)
        {
            var fader = GetFader(sender as DependencyObject);
            fader.Attach();
        }

        private class Fader
        {
            private readonly TextBlock _textBlock;
            private bool _isAttached;
            private LinearGradientBrush _brush;
            private Color _foregroundColor;
            private bool _isClipped;

            public Fader(TextBlock textBlock)
            {
                _textBlock = textBlock;
            }

            public void Attach()
            {
                var parent = VisualTreeHelper.GetParent(_textBlock) as FrameworkElement;  
                if (parent == null || _isAttached)
                {
                    return;
                }

                parent.SizeChanged += UpdateForegroundBrush;
                _textBlock.SizeChanged += UpdateForegroundBrush;

                _foregroundColor = DetermineForegroundColor(_textBlock);
                UpdateForegroundBrush(_textBlock, EventArgs.Empty);

                _textBlock.TextTrimming = TextTrimming.None;

                _isAttached = true;
            }

            public void Detach()
            {
                _textBlock.SizeChanged -= UpdateForegroundBrush;

                var parent = VisualTreeHelper.GetParent(_textBlock) as FrameworkElement;
                if (parent != null)
                {
                    parent.SizeChanged -= UpdateForegroundBrush;
                }

                // remove our explicitly set Foreground color
                _textBlock.ClearValue(TextBlock.ForegroundProperty);
                _isAttached = false;
            }

            private Color DetermineForegroundColor(TextBlock textBlock)
            {
                // if our own Attached Property has been used to set an explicit foreground color, use that
                if (GetForegroundColor(textBlock) != Colors.Transparent)
                {
                    return GetForegroundColor(textBlock);
                }
                
                // otherwise, if the textBlock has inherited a foreground color, use that
                if (textBlock.Foreground is SolidColorBrush)
                {
                    return (textBlock.Foreground as SolidColorBrush).Color;
                }
                
                return Colors.Black;
            }

            private void UpdateForegroundBrush(object sender, EventArgs e)
            {
                // determine if the TextBlock has been clipped
                var layoutClip = LayoutInformation.GetLayoutClip(_textBlock);

                bool needsClipping = layoutClip != null
                    && ((_textBlock.TextWrapping == TextWrapping.NoWrap && layoutClip.Bounds.Width > 0 
                    && layoutClip.Bounds.Width < _textBlock.ActualWidth) 
                    || (_textBlock.TextWrapping == TextWrapping.Wrap && layoutClip.Bounds.Height > 0
                    && layoutClip.Bounds.Height < _textBlock.ActualHeight));

                // if the TextBlock was clipped, but is no longer clipped, then
                // strip all the fancy features
                if (_isClipped && !needsClipping)
                {
                    if (GetShowTextInToolTipWhenTrimmed(_textBlock))
                    {
                        _textBlock.ClearValue(ToolTipService.ToolTipProperty);
                    }

                    _textBlock.Foreground = new SolidColorBrush() { Color = _foregroundColor };
                    _brush = null;
                    _isClipped = false;
                }

                // if the TextBlock has just become clipped, make its
                // content show in its tooltip
                if (needsClipping && GetShowTextInToolTipWhenTrimmed(_textBlock))
                {
                    var toolTip = ToolTipService.GetToolTip(_textBlock) as ToolTip;

                    if (toolTip == null)
                    {
                        toolTip = new ToolTip();
                        ToolTipService.SetToolTip(_textBlock, toolTip);

                        toolTip.Style = _textBlock.GetValue(ToolTipStyleProperty) as Style;
                    }

                    toolTip.Content = _textBlock.Text;                        
                }

                // here's the real magic: if the TextBlock is clipped
                // update its Foreground brush to make it fade out just
                // inside the clip boundary
                if (needsClipping)
                {
                    var visibleWidth = layoutClip.Bounds.Width;
                    var visibleHeight = layoutClip.Bounds.Height;

                    var verticalClip = _textBlock.TextWrapping == TextWrapping.Wrap;

                    if (_brush == null)
                    {
                        _brush = verticalClip ? GetVerticalClipBrush(visibleHeight) : GetHorizontalClipBrush(visibleWidth);
                        _textBlock.Foreground = _brush;
                    }
                    else if (verticalClip && VerticalBrushNeedsUpdating(_brush, visibleHeight))
                    {
                        _brush.EndPoint = new Point(0, visibleHeight);
                        _brush.GradientStops[1].Offset = (visibleHeight - FadeHeight) / visibleHeight;
                    }
                    else if (!verticalClip && HorizontalBrushNeedsUpdating(_brush, visibleWidth))
                    {
                        _brush.EndPoint = new Point(visibleWidth, 0);
                        _brush.GradientStops[1].Offset = (visibleWidth - FadeWidth) / visibleWidth;
                    }

                    _isClipped = true;
                }
            }

            private LinearGradientBrush GetHorizontalClipBrush(double visibleWidth)
            {
                return new LinearGradientBrush
                           {
                               // set MappingMode to absolute so that
                               // we can specify the EndPoint of the brush in
                               // terms of the TextBlock's actual dimensions
                               MappingMode = BrushMappingMode.Absolute,
                               StartPoint = new Point(0, 0),
                               EndPoint = new Point(visibleWidth, 0),
                               GradientStops =
                                   {
                                       new GradientStop()
                                           {Color = _foregroundColor, Offset = 0},
                                       new GradientStop()
                                           {
                                               Color = _foregroundColor,
                                               // Even though the mapping mode is absolute,
                                               // the offset for gradient stops is always relative with
                                               // 0 being the start of the brush, and 1 the end of the brush
                                               Offset = (visibleWidth - FadeWidth)/visibleWidth
                                           },
                                       new GradientStop()
                                           {
                                               Color = Color.FromArgb(0, _foregroundColor.R, _foregroundColor.G, _foregroundColor.B),
                                               Offset = 1
                                           }
                                   }
                           };
            }

            private LinearGradientBrush GetVerticalClipBrush(double visibleHeight)
            {
                return new LinearGradientBrush
                {
                    // set MappingMode to absolute so that
                    // we can specify the EndPoint of the brush in
                    // terms of the TextBlock's actual dimensions
                    MappingMode = BrushMappingMode.Absolute,
                    StartPoint = new Point(0, 0),
                    EndPoint = new Point(0, visibleHeight),
                    GradientStops =
                                   {
                                       new GradientStop()
                                           {Color = _foregroundColor, Offset = 0},
                                       new GradientStop()
                                           {
                                               Color = _foregroundColor,
                                               // Even though the mapping mode is absolute,
                                               // the offset for gradient stops is always relative with
                                               // 0 being the start of the brush, and 1 the end of the brush
                                               Offset = (visibleHeight - FadeHeight)/visibleHeight
                                           },
                                       new GradientStop()
                                           {
                                               Color = Color.FromArgb(0, _foregroundColor.R, _foregroundColor.G, _foregroundColor.B),
                                               Offset = 1
                                           }
                                   }
                };
            }
        }

        private static bool HorizontalBrushNeedsUpdating(LinearGradientBrush brush, double visibleWidth)
        {
            return brush.EndPoint.X < visibleWidth - Epsilon || brush.EndPoint.X > visibleWidth + Epsilon;
        }

        private static bool VerticalBrushNeedsUpdating(LinearGradientBrush brush, double visibleHeight)
        {
            return brush.EndPoint.Y < visibleHeight - Epsilon || brush.EndPoint.Y > visibleHeight + Epsilon;
        }
    }
}
