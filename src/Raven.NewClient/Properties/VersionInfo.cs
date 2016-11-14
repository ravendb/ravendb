using System;

[assembly: RavenVersion(Build = "{build}", CommitHash = "{commit}", Version = "4.0")]

[AttributeUsage(AttributeTargets.Assembly)]
public class RavenVersionAttribute : Attribute
{
    public string CommitHash { get; set; }
    public string Build { get; set; }
    public string Version { get; set; }
}