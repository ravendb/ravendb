import { Controller, ControllerRenderProps, useFormContext, useWatch } from "react-hook-form";
import { SetupWizardFormData } from "../setupWizardValidation";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import FileDropzone from "components/common/FileDropzone";
import { FormGroup, FormLabel, FormSelect } from "components/common/Form";
import { useAsync } from "react-async-hook";
import { useServices } from "components/hooks/useServices";
import messagePublisher from "common/messagePublisher";
import { SelectOption } from "components/common/select/Select";
import { useEffect, useMemo } from "react";
import RichAlert from "components/common/RichAlert";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { setupWizardGA4Prefixes } from "components/setupWizard/utils/setupWizardConstants";
import { useEventsCollector } from "hooks/useEventsCollector";
import { base64ToFile, fileToBase64 } from "components/setupWizard/utils/setupWizardUtils";
import { LazyLoad } from "components/common/LazyLoad";
import { SetupWizardInfoPopover } from "components/setupWizard/partials/SetupWizardInfoPopover";
import { setupWizardFormDefaultValues } from "components/setupWizard/utils/setupWizardFormDefaultValues";

export function SetupWizardUsePackageStep() {
    const { control, setValue, watch } = useFormContext<SetupWizardFormData>();
    const {
        usePackageStep: { fileZip, fileName, isZipValid },
    } = useWatch({ control });

    const { setupWizardService } = useServices();

    const asyncExtractNodeInfos = useAsync(async () => {
        if (!fileZip) {
            return [];
        }

        return await setupWizardService.extractNodesInfoFromPackage(fileZip);
    }, [fileZip]);

    const nodeInfos = useMemo(() => {
        return asyncExtractNodeInfos.result ?? [];
    }, [asyncExtractNodeInfos.result]);

    const nodeOptions: SelectOption[] = nodeInfos.map((nodeInfo) => ({
        value: nodeInfo.Tag,
        label: `Node ${nodeInfo.Tag} (${nodeInfo.PublicServerUrl || nodeInfo.ServerUrl})`,
    }));

    const handleNodeTagChange = (newValue: SelectOption) => {
        setValue("usePackageStep.nodeTag", newValue.value);
        const nodeInfo = nodeInfos.find((x) => x.Tag === newValue.value);
        setValue("usePackageStep.serverUrl", nodeInfo.ServerUrl);
        setValue("usePackageStep.publicServerUrl", nodeInfo.PublicServerUrl);
    };

    useEffect(() => {
        if (nodeInfos.length === 1) {
            const singleNode = nodeInfos[0];
            setValue("usePackageStep.nodeTag", singleNode.Tag);
            setValue("usePackageStep.serverUrl", singleNode.ServerUrl);
            setValue("usePackageStep.publicServerUrl", singleNode.PublicServerUrl);
        }
    }, [nodeInfos, setValue]);

    useEffect(() => {
        const subscribe = watch((values, { name }) => {
            if (name === "usePackageStep.nodeTag") {
                const nodeInfo = nodeInfos.find((x) => x.Tag === values.usePackageStep.nodeTag);
                setValue("usePackageStep.publicServerUrl", nodeInfo?.PublicServerUrl);
                setValue("usePackageStep.serverUrl", nodeInfo?.ServerUrl);
            }
        });

        return () => {
            subscribe.unsubscribe();
        };
    }, [watch, setValue, nodeInfos]);

    // Handle isZipSecure
    useEffect(() => {
        if (!nodeInfos.length) {
            setValue("usePackageStep.isZipSecure", false);
            return;
        }

        setValue("usePackageStep.isZipSecure", nodeInfos.every(isNodeSecure));
    }, [nodeInfos, setValue]);

    useEffect(() => {
        // packages should have any node
        setValue("usePackageStep.isZipValid", nodeInfos.length !== 0);
    }, [nodeInfos, setValue]);

    const getInitialFiles = useMemo((): File[] => {
        if (fileZip) {
            try {
                return [base64ToFile(fileZip, fileName, "application/zip")];
            } catch (error) {
                messagePublisher.reportError("Failed to load file", error);
                return [];
            }
        }
        return [];
    }, [fileZip]);

    const clearFile = () => {
        setValue("usePackageStep.fileName", "");
        setValue("usePackageStep.fileZip", "");
    };

    const handleFileChange = async (files: File[], field: ControllerRenderProps<SetupWizardFormData>) => {
        const file = files[0];

        if (!file.name.trim()) {
            clearFile();
            messagePublisher.reportError("Failed to load file");
            return;
        }

        try {
            const fileInString = await fileToBase64(file);
            const cleanFileInBase64 = fileInString.substring(fileInString.indexOf(",") + 1);

            setValue("usePackageStep.fileZip", cleanFileInBase64, {
                shouldDirty: true,
            });
            setValue("usePackageStep.fileName", file.name, {
                shouldDirty: true,
            });

            field.onChange(cleanFileInBase64);
        } catch (e) {
            clearFile();
            messagePublisher.reportError("Failed to load file", e.message);
        }
    };

    return (
        <div>
            <h2 className="mb-1">Use setup package</h2>
            <p className="mb-4 text-muted">
                Use an existing setup package to configure selected nodes in your cluster.
            </p>
            <FormGroup>
                <Controller
                    name="usePackageStep.fileZip"
                    control={control}
                    render={({ field }) => (
                        <FileDropzone
                            maxFiles={1}
                            validExtensions={["zip"]}
                            initialFiles={getInitialFiles}
                            {...field}
                            onChange={(files) => handleFileChange(files, field)}
                        />
                    )}
                />
            </FormGroup>
            <LazyLoad active={asyncExtractNodeInfos.loading}>
                {fileZip && isZipValid && asyncExtractNodeInfos.status === "success" && (
                    <FormGroup>
                        <FormLabel className="hstack">
                            <div>Node tag</div>
                            <PopoverWithHoverWrapper
                                message={
                                    <SetupWizardInfoPopover
                                        description="Select which node from the predefined cluster you would like to set up now.
                                            Do not choose one that is already configured."
                                        ravenLinkHash="WJJHFY"
                                    />
                                }
                            >
                                <Icon icon="info-new" />
                            </PopoverWithHoverWrapper>
                        </FormLabel>
                        <FormSelect
                            control={control}
                            name="usePackageStep.nodeTag"
                            placeholder="Select node tag"
                            onChange={handleNodeTagChange}
                            options={nodeOptions}
                        />
                    </FormGroup>
                )}
                {!isZipValid && fileZip && (
                    <RichAlert variant="danger">Invalid nodes configuration in zip file.</RichAlert>
                )}
            </LazyLoad>
        </div>
    );
}

export function SetupWizardUsePackageStepFooter() {
    const { setValue, control } = useFormContext<SetupWizardFormData>();
    const { reportEvent } = useEventsCollector();

    const {
        usePackageStep: { isZipValid, fileZip, nodeTag },
    } = useWatch({ control });

    const handleContinue = () => {
        setValue("currentStep", "Finish");
    };

    const handleBack = () => {
        reportEvent(setupWizardGA4Prefixes.usePackageStep, "back");
        setValue("currentStep", "Setup method");
        setValue("usePackageStep", setupWizardFormDefaultValues["usePackageStep"]);
    };

    return (
        <div className="d-flex justify-content-between">
            <Button variant="secondary" className="rounded-pill" onClick={handleBack}>
                <Icon icon="arrow-left" /> Back
            </Button>
            <Button
                variant="primary"
                className="rounded-pill"
                onClick={handleContinue}
                disabled={!isZipValid || !fileZip || !nodeTag}
            >
                Continue <Icon icon="arrow-right" margin="m-0" />
            </Button>
        </div>
    );
}

const isNodeSecure = (node: Raven.Server.Web.System.ConfigurationNodeInfo): boolean => {
    return node.PublicServerUrl && node.PublicServerUrl.startsWith(securePrefix);
};

const securePrefix = "https://";
