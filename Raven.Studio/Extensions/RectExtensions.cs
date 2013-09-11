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

namespace Raven.Studio.Extensions
{
    public static class RectExtensions
    {
        public static Rect Inflate(this Rect value, int inflateBy)
        {
            return new Rect(new Point(value.Left - inflateBy, value.Top - inflateBy), new Size(value.Width + inflateBy * 2, value.Height + inflateBy * 2));
        }
    }
}
