namespace TutorialSamples
{
    using System;
    /// <summary>
    /// Summary description for Prime
    /// </summary>
    public class Prime
    {
        public static bool IsPrime(int num){
            if (num == 0)
            {
                return false;
            }

            for (int i = 2; i < (int)Math.Sqrt(num); i++)
            {
                if (num % i == 0)
                {
                    return false;
                }
            }

            return true;
        }
    }
}
