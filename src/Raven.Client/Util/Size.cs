using System;

namespace Raven.Client.Util
{
    public sealed class Size
    {
        internal const double GB = 1024 * 1024 * 1024;
        const double MB = 1024 * 1024;
        const double KB = 1024;
        
        public Size()
        {
            SizeInBytes = 0;
        }
        
        public Size(long sizeInBytes)
        {
            SizeInBytes = sizeInBytes;
        }

        public long SizeInBytes { get; set; }

        public string HumaneSize => Humane(SizeInBytes);

        public static string Humane(long? size)
        {
            if (size == null)
                return null;
            
            var absSize = Math.Abs(size.Value);
            
            if (absSize == 0)
                return "0 Bytes";

            if (absSize > GB) // GB
                return string.Format("{0:#,#.##} GBytes", size / GB);
            if (absSize > MB)
                return string.Format("{0:#,#.##} MBytes", size / MB);
            if (absSize > KB)
                return string.Format("{0:#,#.##} KBytes", size / KB);
            return string.Format("{0:#,#0} Bytes", size);
        }

        internal static Size Parse(string humanSize)
        {
            if (string.IsNullOrWhiteSpace(humanSize))
                return new Size(0);

            double multiplier;
            
            humanSize = humanSize.Trim();
    
            if (humanSize.EndsWith("GBytes"))
                multiplier = GB;
            else if (humanSize.EndsWith("MBytes"))
                multiplier = MB;
            else if (humanSize.EndsWith("KBytes"))
                multiplier = KB;
            else if (humanSize.EndsWith("Bytes"))
                multiplier = 1;
            else
                throw new ArgumentException("Provided size must end with unit", nameof(humanSize));
            
            int spaceIndex = humanSize.IndexOf(' ');
            if (spaceIndex == -1)
                throw new ArgumentException("Size must contain a space", nameof(humanSize));
                
            var numericPart = humanSize.Substring(0, spaceIndex);
    
            numericPart = numericPart.Replace(",", "");
    
            if (double.TryParse(numericPart, out double value))
                return new Size((long)(value * multiplier));
    
            return new Size(0);
        }

    }
}
