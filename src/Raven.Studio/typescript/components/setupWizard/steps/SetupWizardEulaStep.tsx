import { Icon } from "components/common/Icon";
import { useServices } from "components/hooks/useServices";
import { useAsync } from "react-async-hook";
import Button from "react-bootstrap/Button";
import { useFormContext } from "react-hook-form";
import { SetupWizardFormData } from "../setupWizardValidation";
import genUtils from "common/generalUtils";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { LazyLoad } from "components/common/LazyLoad";
import { useDispatch } from "react-redux";
import { setupWizardActions, setupWizardSelectors } from "../store/setupWizardSlice";
import { useAppSelector } from "components/store";
import { useCallback, useEffect } from "react";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { setupWizardGA4Prefixes } from "components/setupWizard/utils/setupWizardConstants";

type StorybookGlobalWindow = Window & typeof globalThis & { storybookMode?: boolean };

export function SetupWizardEulaStep() {
    const dispatch = useDispatch();
    const { setupWizardService } = useServices();
    const { reportEvent } = useEventsCollector();

    const asyncGetEula = useAsync(setupWizardService.getEula, []);

    useEffect(() => {
        if ((window as StorybookGlobalWindow).storybookMode) {
            dispatch(setupWizardActions.isEulaScrolledToBottomSet(true));
        }
    }, []);

    const handleScroll = useCallback(
        (node: React.UIEvent<HTMLDivElement, UIEvent>) => {
            if (genUtils.isScrolledToBottom(node.target as HTMLElement)) {
                dispatch(setupWizardActions.isEulaScrolledToBottomSet(true));
                reportEvent(setupWizardGA4Prefixes.eulaStep, "scroll", "reached-bottom");
            }
        },
        [dispatch, reportEvent]
    );

    return (
        <div className="vstack flex-grow eula-step">
            <h2 className="mb-1">Read the EULA (End-User License Agreement)</h2>
            <p className="mb-4 text-muted">
                The following license agreement must be accepted in order to use this software.
            </p>
            <div className="eula-container mb-4" onScroll={handleScroll}>
                <LazyLoad active={asyncGetEula.loading}>
                    <pre className="eula-content">{asyncGetEula.result ?? ""}</pre>
                    <div data-testid="eula-bottom" />
                </LazyLoad>
            </div>
        </div>
    );
}

export function SetupWizardEulaStepFooter() {
    const { setValue } = useFormContext<SetupWizardFormData>();
    const { reportEvent } = useEventsCollector();

    const isScrolledToBottom = useAppSelector(setupWizardSelectors.isEulaScrolledToBottom);

    const handleContinue = () => {
        reportEvent(setupWizardGA4Prefixes.eulaStep, "continue", "accepted");
        setValue("currentStep", "Setup method");
    };

    return (
        <div className="d-flex justify-content-end">
            <ConditionalPopover
                conditions={{
                    isActive: !isScrolledToBottom,
                    message: "Review the EULA to enable the button.",
                }}
                popoverPlacement="top"
            >
                <Button
                    variant="primary"
                    className="rounded-pill"
                    onClick={handleContinue}
                    disabled={!isScrolledToBottom}
                >
                    Continue <Icon icon="arrow-right" margin="m-0" />
                </Button>
            </ConditionalPopover>
        </div>
    );
}
