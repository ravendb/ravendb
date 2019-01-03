using System;
using System.Reflection;
using System.Resources;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

[assembly: AssemblyCompany("Hibernating Rhinos")]
[assembly: AssemblyCopyright("© Hibernating Rhinos 2004 - 2017 All rights reserved.")]
[assembly: AssemblyTrademark("")]
[assembly: ComVisible(false)]
[assembly: CLSCompliant(true)]
[assembly: AssemblyTitle("RavenDB")]
[assembly: AssemblyVersion("3.5.9")]
[assembly: AssemblyFileVersion("3.5.9.13")]
[assembly: AssemblyInformationalVersion("3.5.9")]
[assembly: AssemblyProduct("RavenDB")]
[assembly: AssemblyDescription("RavenDB is a second generation LINQ enabled document database for .NET")]

#if !DNXCORE50
[assembly: SuppressIldasm]
#endif

#if DEBUG
[assembly: AssemblyConfiguration("Debug")]
#else
[assembly: AssemblyConfiguration("Release")]
#endif

[assembly: AssemblyDelaySign(false)]
[assembly: NeutralResourcesLanguage("en-US")]
[assembly: RavenVersion(Build = "{build-label}", CommitHash = "{commit}", Version = "3.5")]

[AttributeUsage(AttributeTargets.Assembly)]
public class RavenVersionAttribute : Attribute
{
    private string build;
    public string CommitHash { get; set; }
    public string Build
    {
        get
        {
            int _;
            if (int.TryParse(build, out _) == false)
                return "13";
            return build;
        }
        set { build = value; }
    }
    public string Version { get; set; }
}
