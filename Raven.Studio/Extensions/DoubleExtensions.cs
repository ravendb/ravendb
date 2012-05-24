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
    public static class DoubleExtensions
    {
        public static bool IsCloseTo(this double value, double other, double epsilon = 0.0000001)
        {
            return Math.Abs(value - other) < epsilon;
        }
    }
}
