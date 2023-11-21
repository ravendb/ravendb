namespace Tests.Infrastructure
{
    public class NightlyBuildMultiplatformFactAttribute : NightlyBuildFactAttribute
    {
        private readonly RavenPlatform _platform;
        private readonly RavenArchitecture _architecture;
        private readonly RavenIntrinsics _intrinsics;

        public NightlyBuildMultiplatformFactAttribute(RavenPlatform platform = RavenPlatform.All)
            : this(platform, RavenArchitecture.All)
        {
        }

        public NightlyBuildMultiplatformFactAttribute(RavenArchitecture architecture = RavenArchitecture.All)
            : this(RavenPlatform.All, architecture)
        {
        }
        
        public NightlyBuildMultiplatformFactAttribute(RavenIntrinsics intrinsics = RavenIntrinsics.None)
            : this(RavenPlatform.All, RavenArchitecture.All, intrinsics)
        {
        }

        public NightlyBuildMultiplatformFactAttribute(RavenPlatform platform = RavenPlatform.All, RavenArchitecture architecture = RavenArchitecture.All, RavenIntrinsics intrinsics = RavenIntrinsics.None)
        {
            _platform = platform;
            _architecture = architecture;
            _intrinsics = intrinsics;
        }

        public bool LicenseRequired { get; set; }

        public bool NightlyBuildOnly { get; set; }

        public override string Skip
        {
            get
            {
                var skip = base.Skip;
                if (skip != null)
                    return skip;

                return RavenMultiplatformFactAttribute.ShouldSkip(_platform, _architecture, _intrinsics, LicenseRequired, NightlyBuildOnly);
            }
        }
    }
}
