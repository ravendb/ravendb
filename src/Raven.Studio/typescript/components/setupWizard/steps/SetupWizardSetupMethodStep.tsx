import { useFormContext, useWatch } from "react-hook-form";
import { SetupWizardFormData } from "../setupWizardValidation";
import SetupWizardClickableCard from "../partials/SetupWizardClickableCard";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import assertUnreachable from "components/utils/assertUnreachable";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { SetupWizardStepItem } from "components/setupWizard/partials/SetupWizardStepItem";
import { NumberedList } from "components/common/NumberedList";
import { useEffect } from "react";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { setupWizardGA4Prefixes } from "components/setupWizard/utils/setupWizardConstants";
import { SetupWizardInfoPopover } from "components/setupWizard/partials/SetupWizardInfoPopover";

export function SetupWizardSetupMethodStep() {
    const { control, setValue } = useFormContext<SetupWizardFormData>();
    const {
        setupMethodStep: { method: selectedMethod },
    } = useWatch({ control });
    const { reportEvent } = useEventsCollector();

    useEffect(() => {
        setValue("setupMethodStep.method", selectedMethod ?? "newCluster");
    }, []);

    return (
        <div>
            <h2 className="mb-1">Choose your setup method</h2>
            <p className="mb-4 text-muted">
                This wizard will assist you with setting up your RavenDB server. You can set up a new cluster, create an
                external configuration package, or continue with an existing setup package.
            </p>
            <div className="mt-4">
                <h5 className="mb-1">I&apos;m just starting</h5>
                <SetupWizardClickableCard
                    icon="server"
                    title="Set up new cluster"
                    description="Create a completely new cluster with fresh configurations"
                    isSelected={selectedMethod === "newCluster"}
                    onClick={() => {
                        setValue("setupMethodStep.method", "newCluster");
                        reportEvent(setupWizardGA4Prefixes.setupMethod, "select-method", "newCluster");
                    }}
                    popoverMessage={
                        <ul className="mb-0 ps-3">
                            <li>Deploying RavenDB for the first time</li>
                            <li>Setting up a new single-node or multi-node cluster</li>
                            <li>Creating a fresh cluster with a new configuration</li>
                        </ul>
                    }
                />
                <SetupWizardClickableCard
                    className="mt-2"
                    icon="default"
                    title="Create package for external setup"
                    description="Generate an external setup package during configuration for customized deployment"
                    isSelected={selectedMethod === "createPackage"}
                    onClick={() => {
                        setValue("setupMethodStep.method", "createPackage");
                        reportEvent(setupWizardGA4Prefixes.setupMethod, "select-method", "createPackage");
                    }}
                    popoverMessage={
                        <ul className="mb-0 ps-3">
                            <li>
                                You want to create a Package for an external environment i.e. cloud instance,
                                containers, or similar
                            </li>
                            <li>Creating pre-configured package without setting up a server</li>
                            <li>Useful with offline or remote setup</li>
                        </ul>
                    }
                />
            </div>
            <div className="my-4">
                <h5 className="mb-1">I have something to work with</h5>
                <SetupWizardClickableCard
                    icon="default"
                    addon="arrow-up"
                    title="Use setup package"
                    description="Deploy the cluster using a predefined setup package with default or minimal configurations"
                    isSelected={selectedMethod === "usePackage"}
                    onClick={() => {
                        setValue("setupMethodStep.method", "usePackage");
                        reportEvent(setupWizardGA4Prefixes.setupMethod, "select-method", "usePackage");
                    }}
                    popoverMessage={
                        <>
                            <ul className="ps-3">
                                <li>Setting up another node in an existing cluster</li>
                                <li>Setting up a new cluster from external package</li>
                            </ul>
                            <p className="mb-0">
                                To obtain a setup package you need to setup a new multi-node cluster or create package
                                for external setup.
                            </p>
                        </>
                    }
                />
            </div>
        </div>
    );
}

export function SetupWizardSetupMethodStepFooter() {
    const { control, setValue } = useFormContext<SetupWizardFormData>();

    const {
        setupMethodStep: { method: selectedMethod },
    } = useWatch({ control });

    const { reportEvent } = useEventsCollector();

    const handleContinue = async () => {
        // Report continue with selected method
        if (selectedMethod) {
            reportEvent(setupWizardGA4Prefixes.setupMethod, "continue", selectedMethod);
        }
        switch (selectedMethod) {
            case "newCluster":
            case "createPackage":
                setValue("currentStep", "License key");
                break;
            case "usePackage":
                setValue("currentStep", "Use setup package");
                break;
            default:
                assertUnreachable(selectedMethod);
        }
    };

    return (
        <div className="d-flex justify-content-between">
            <PopoverWithHoverWrapper
                message={
                    <SetupWizardInfoPopover
                        description={
                            <NumberedList>
                                <SetupWizardStepItem stepIndicator={1}>
                                    <span>
                                        Open the <em>settings.json</em> file located in your RavenDB installation
                                        directory
                                    </span>
                                </SetupWizardStepItem>
                                <SetupWizardStepItem stepIndicator={2}>
                                    <span>
                                        Change the setup mode to None, e.g.{" "}
                                        <code>&#34;Setup.Mode&#34;: &#34;None&#34;</code>
                                    </span>
                                </SetupWizardStepItem>
                            </NumberedList>
                        }
                        docsLink="https://docs.ravendb.net/start/installation/manual"
                    />
                }
            >
                <span className="md-label mb-0">
                    <Icon icon="info" />
                    How to set up manually?
                </span>
            </PopoverWithHoverWrapper>
            <ConditionalPopover
                conditions={{
                    isActive: !selectedMethod,
                    message: "You need to complete this step to go forward.",
                }}
                popoverPlacement="top"
            >
                <Button variant="primary" className="rounded-pill" onClick={handleContinue} disabled={!selectedMethod}>
                    Continue <Icon icon="arrow-right" margin="m-0" />
                </Button>
            </ConditionalPopover>
        </div>
    );
}
