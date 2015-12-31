using System;
using System.Reflection;
using System.Resources;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

[assembly: AssemblyCompany("Hibernating Rhinos")]
[assembly: AssemblyCopyright("© Hibernating Rhinos 2004 - 2016 All rights reserved.")]
[assembly: AssemblyTrademark("")]
[assembly: ComVisible(false)]
[assembly: CLSCompliant(false)]
[assembly: AssemblyTitle("RavenDB")]
[assembly: AssemblyVersion("4.0.0")]
[assembly: AssemblyFileVersion("4.0.13.0")]
[assembly: AssemblyInformationalVersion("4.0.0 / {commit} / ")]
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
[assembly: RavenVersion(Build = ".13.", CommitHash = "{commit}")]

[AttributeUsage(AttributeTargets.Assembly)]
public class RavenVersionAttribute : Attribute
{
    public string CommitHash { get; set; }
    public string Build { get; set; }
}