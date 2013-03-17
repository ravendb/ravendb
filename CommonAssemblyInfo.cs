using System;
using System.Reflection;
using System.Resources;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

[assembly: AssemblyCompany("Hibernating Rhinos")]
[assembly: AssemblyCopyright("© Hibernating Rhinos 2004 - 2.13. All rights reserved.")]
[assembly: AssemblyTrademark("")]
[assembly: ComVisible(false)]

[assembly: AssemblyTitle("RavenDB")]
[assembly: AssemblyVersion("2.5.0")]
[assembly: AssemblyFileVersion("2.5.13.0")]
[assembly: AssemblyInformationalVersion("2.5.0 / 6dce79a")]
[assembly: AssemblyProduct("RavenDB")]
[assembly: AssemblyDescription("A second generation LINQ enabled document database for .NET")]

#if SILVERLIGHT
[assembly: CLSCompliant(false)]
#else
[assembly: CLSCompliant(true)]
#endif

#if !SILVERLIGHT && !NETFX_CORE
[assembly: SuppressIldasm()]
#endif

#if DEBUG
[assembly: AssemblyConfiguration("Debug")]
#else
[assembly: AssemblyConfiguration("Release")]
#endif

[assembly: AssemblyDelaySign(false)]
[assembly: NeutralResourcesLanguage("en-US")]
