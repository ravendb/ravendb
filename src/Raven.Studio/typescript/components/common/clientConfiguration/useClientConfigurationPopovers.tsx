import { useState } from "react";

export default function useClientConfigurationPopovers() {
    const [identityPartsSeparator, setIdentityPartsSeparator] = useState<HTMLElement>();
    const [maximumRequestsPerSession, setMaximumRequestsPerSession] = useState<HTMLElement>();
    const [sessionContext, setSessionContext] = useState<HTMLElement>();
    const [readBalanceBehavior, setReadBalanceBehavior] = useState<HTMLElement>();
    const [loadBalanceSeedBehavior, setLoadBalanceSeedBehavior] = useState<HTMLElement>();
    const [effectiveConfiguration, setEffectiveConfiguration] = useState<HTMLElement>();

    return {
        identityPartsSeparator,
        setIdentityPartsSeparator,
        maximumRequestsPerSession,
        setMaximumRequestsPerSession,
        sessionContext,
        setSessionContext,
        readBalanceBehavior,
        setReadBalanceBehavior,
        loadBalanceSeedBehavior,
        setLoadBalanceSeedBehavior,
        effectiveConfiguration,
        setEffectiveConfiguration,
    };
}
