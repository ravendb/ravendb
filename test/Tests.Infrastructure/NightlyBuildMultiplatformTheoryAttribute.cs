namespace Tests.Infrastructure
{
    public class NightlyBuildMultiplatformTheoryAttribute : NightlyBuildTheoryAttribute
    {
        private readonly RavenPlatform _platform;
        private readonly RavenArchitecture _architecture;
        private readonly RavenIntrinsics _intrinsics;

        public NightlyBuildMultiplatformTheoryAttribute(RavenPlatform platform = RavenPlatform.All)
            : this(platform, RavenArchitecture.All)
        {
        }

        public NightlyBuildMultiplatformTheoryAttribute(RavenArchitecture architecture = RavenArchitecture.All)
            : this(RavenPlatform.All, architecture)
        {
        }
        
        public NightlyBuildMultiplatformTheoryAttribute(RavenIntrinsics intrinsics = RavenIntrinsics.None)
            : this(RavenPlatform.All, RavenArchitecture.All, intrinsics)
        {
        }

        public NightlyBuildMultiplatformTheoryAttribute(RavenPlatform platform = RavenPlatform.All, RavenArchitecture architecture = RavenArchitecture.All, RavenIntrinsics intrinsics = RavenIntrinsics.None)
        {
            _platform = platform;
            _architecture = architecture;
            _intrinsics = intrinsics;
        }

        public bool LicenseRequired { get; set; }

        public override string Skip
        {
            get
            {
                var skip = base.Skip;
                if (skip != null)
                    return skip;

                return RavenMultiplatformFactAttribute.ShouldSkip(_platform, _architecture, _intrinsics, LicenseRequired, nightlyBuildOnly: true);
            }
        }
    }
}
