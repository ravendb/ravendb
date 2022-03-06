namespace Tests.Infrastructure
{
    public class NightlyBuildMultiplatformFactAttribute : NightlyBuildFactAttribute
    {
        private readonly RavenPlatform _platform;
        private readonly RavenArchitecture _architecture;

        public NightlyBuildMultiplatformFactAttribute(RavenPlatform platform = RavenPlatform.All)
            : this(platform, RavenArchitecture.All)
        {
        }

        public NightlyBuildMultiplatformFactAttribute(RavenArchitecture architecture = RavenArchitecture.All)
            : this(RavenPlatform.All, architecture)
        {
        }

        public NightlyBuildMultiplatformFactAttribute(RavenPlatform platform = RavenPlatform.All, RavenArchitecture architecture = RavenArchitecture.All)
        {
            _platform = platform;
            _architecture = architecture;
        }

        public bool LicenseRequired { get; set; }

        public override string Skip
        {
            get
            {
                var skip = base.Skip;
                if (skip != null)
                    return skip;

                return MultiplatformFactAttribute.ShouldSkip(_platform, _architecture, LicenseRequired);
            }
        }
    }
}
