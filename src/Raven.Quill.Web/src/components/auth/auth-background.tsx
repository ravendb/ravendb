import * as React from "react";

const Beams = React.lazy(() => import("@/components/auth/backgrounds/beams"));

// A muted coral lifts the black beam geometry off the near-black background. Deliberately duller
// than the ramp (brand-400 is #f08c6f) so the login screen stays quiet behind the form.
const BEAM_TINT = "#9b6b5d";

function usePrefersReducedMotion(): boolean {
    const query = "(prefers-reduced-motion: reduce)";
    const [reduced, setReduced] = React.useState(
        () => typeof window !== "undefined" && window.matchMedia(query).matches,
    );

    React.useEffect(() => {
        const mediaQuery = window.matchMedia(query);
        const handleChange = () => setReduced(mediaQuery.matches);

        handleChange();
        mediaQuery.addEventListener("change", handleChange);

        return () => mediaQuery.removeEventListener("change", handleChange);
    }, []);

    return reduced;
}

const BrandGlow = ({ strength = 16 }: { strength?: number }) => (
    <div
        aria-hidden
        className="pointer-events-none absolute inset-0"
        style={{
            background: `radial-gradient(70% 55% at 50% -10%, color-mix(in oklch, var(--brand-500) ${strength}%, transparent), transparent 70%)`,
        }}
    />
);

const LegibilityScrim = () => (
    <div
        aria-hidden
        className="pointer-events-none absolute inset-0"
        style={{
            background:
                "radial-gradient(46% 44% at 50% 50%, var(--background) 0%, color-mix(in oklch, var(--background) 55%, transparent) 45%, transparent 80%)",
        }}
    />
);

const DarkBeams = () => (
    <Beams
        lightColor={BEAM_TINT}
        beamNumber={30}
        beamWidth={2.5}
        beamHeight={14}
        speed={1.25}
        noiseIntensity={1}
        scale={0.2}
        rotation={-15}
    />
);

export function AuthBackground() {
    const prefersReducedMotion = usePrefersReducedMotion();

    if (prefersReducedMotion) {
        return <BrandGlow />;
    }

    return (
        <div aria-hidden className="pointer-events-none absolute inset-0">
            <React.Suspense fallback={<BrandGlow />}>
                <DarkBeams />
            </React.Suspense>
            <LegibilityScrim />
            <BrandGlow strength={10} />
        </div>
    );
}
