using System;
using System.Reflection;
using System.Resources;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

[assembly: AssemblyCompany("Hibernating Rhinos")]
[assembly: AssemblyCopyright("© Hibernating Rhinos 2004 - 2012. All rights reserved.")]
[assembly: AssemblyTrademark("")]
[assembly: ComVisible(false)]

[assembly: AssemblyVersion("2.0.0")]
[assembly: AssemblyFileVersion("2.0.13.0")]
[assembly: AssemblyInformationalVersion("2.0.0 / {commit}")]
[assembly: AssemblyProduct("RavenDB")]
[assembly: AssemblyDescription("A second generation LINQ enabled document database for .NET")]

#if SILVERLIGHT
[assembly: CLSCompliant(false)]
#else
[assembly: SuppressIldasm()]
[assembly: CLSCompliant(true)]
#endif

#if DEBUG
[assembly: AssemblyConfiguration("Debug")]
#else
[assembly: AssemblyConfiguration("Release")]
#endif

[assembly: AssemblyDelaySign(false)]
[assembly: NeutralResourcesLanguage("en-US")]