using System.Runtime.InteropServices;

namespace Tests.Infrastructure
{
    public class Windows64BitTheoryAttribute : Theory64BitAttribute
    {
        public override string Skip
        {
            get
            {
                if (base.Skip != null)
                    return base.Skip;

                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) == false)
                    return "Test can be run on Windows machine only";

                return null;
            }
        }
    }
}
